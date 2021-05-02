package mr

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	MapTask TaskType = iota
	ReduceTask
	WaitTask
	ExitTask
)

const (
	MaxWaitTime = 10 * time.Second
)

type TaskType int

type Master struct {
	// Your definitions here.
	// Task ID -> file
	mapTasks []Task
	mapLock sync.Mutex

	reduceTasks []Task
	reduceLock sync.Mutex

	mapTaskLeft int32
	reduceLeft  int32
	nReduce     int
}

type Task struct {
	Type TaskType
	ID   int
	Filename string
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m* Master) GetTask(args *TaskArgs, reply *TaskReply) error {
	m.mapLock.Lock()
	if len(m.mapTasks) != 0 {
		//assign map task
		task := m.mapTasks[0]
		m.mapTasks = m.mapTasks[1:]
		reply.Task = task
		reply.NReduce = m.nReduce
		m.mapLock.Unlock()
		go m.checkMapDone(task)
		return nil
	}
	m.mapLock.Unlock()
	if atomic.LoadInt32(&m.mapTaskLeft) == 0 {
		m.reduceLock.Lock()
		if len(m.reduceTasks) != 0 {
			//assign reduce task
			reduceTask := m.reduceTasks[0]
			m.reduceTasks = m.reduceTasks[1:]
			reply.Task = reduceTask
			m.reduceLock.Unlock()
			go m.checkReduceDone(reduceTask)
			return nil
		}
		m.reduceLock.Unlock()
		if atomic.LoadInt32(&m.reduceLeft) == 0 {
			// please exit
			reply.Task = Task{Type: ExitTask}
		} else {
			// wait for 1 sec
			fmt.Println("reduce not done yet")
			reply.Task = Task{Type: WaitTask}
		}
	} else {
		// wait for 1 sec
		fmt.Println("map not done yet")
		reply.Task = Task{Type: WaitTask}
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	// Your code here.
	return atomic.LoadInt32(&m.reduceLeft) == 0
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		mapTasks:    make([]Task, len(files)),
		reduceTasks: make([]Task, nReduce),
		nReduce:     nReduce,
		mapTaskLeft: int32(len(files)),
		reduceLeft:  int32(nReduce),
	}
	// Your code here.
	for i, f := range files {
		m.mapTasks[i] = Task{
			Type:     MapTask,
			ID:       i,
			Filename: f,
		}
	}

	for i := 0; i < nReduce; i++ {
		m.reduceTasks[i] = Task{
			Type:     ReduceTask,
			ID:       i,
			Filename: fmt.Sprintf("mr-out-%d", i),
		}
	}

	m.server()
	return &m
}

func (m *Master) checkMapDone(task Task) bool {
	// check if files "mr-taskId-[0 to nReduce] exist
	waitTime := 0 * time.Second
	for waitTime < MaxWaitTime {
		finish := true
		for i := 0; i < m.nReduce; i++ {
			if _, err := os.Stat(fmt.Sprintf("mr-worker%d-%d", task.ID, i)); os.IsNotExist(err) {
				finish = false
				break
			}
		}
		if finish {
			// if task is done, we should put task into finished one
			atomic.AddInt32(&m.mapTaskLeft, -1)
			fmt.Printf("map task %v is done\n", task.ID)
			return true
		}
		time.Sleep(1 * time.Second)
		waitTime += 1 * time.Second
	}
	// not finished before MaxWaitTime
	m.mapLock.Lock()
	defer m.mapLock.Unlock()
	m.mapTasks = append(m.mapTasks, task)
	return false
}

func (m *Master) checkReduceDone(task Task) bool {
	// check if files "mr-out-reduceId exist
	waitTime := 0 * time.Second
	for waitTime < MaxWaitTime {
		finished := true
		if _, err := os.Stat(fmt.Sprintf("mr-out-%d", task.ID)); os.IsNotExist(err) {
			// if not exist, we should put task into unfinished one
			finished = false
			break
		}
		if finished {
			// if task is done, we should reduce leftTask by 1
			atomic.AddInt32(&m.reduceLeft, -1)
			fmt.Printf("reduce task %v is done\n", task.ID)
			return true
		}
		time.Sleep(1 * time.Second)
		waitTime += 1 * time.Second
	}
	m.reduceLock.Lock()
	defer m.reduceLock.Unlock()
	m.reduceTasks = append(m.reduceTasks, task)
	return false
}

