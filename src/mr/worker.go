package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	fmt.Println("worker starts")
	for {
		reply := CallGetTask()
		switch reply.Task.Type {
		case ExitTask:
			fmt.Println("job is done, exit worker")
			os.Exit(0)
		case WaitTask:
			time.Sleep(1 * time.Second)
			continue
		case MapTask:
			//map task
			mapTask(reply.Task, reply.NReduce, mapf)
		case ReduceTask:
			//reduce task
			reduceTask(reply.Task, reducef)
		}
	}
}

func mapTask(task Task, nReduce int, mapf func(string, string) []KeyValue) {
	// read each input file,
	// pass it to Map,
	// accumulate the intermediate Map output.
	//
	fmt.Printf("start mapTask %v with filename %v\n", task.ID, task.Filename)
	file, err := os.Open(task.Filename)
	if err != nil {
		log.Fatalf("cannot open %v", task.Filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.Filename)
	}
	file.Close()
	kva := mapf(task.Filename, string(content))
	sort.Sort(ByKey(kva))

	// partition into nReduce buckets
	intermediate := make([][]KeyValue, nReduce)
	for _, kv := range kva {
		idx := ihash(kv.Key) % nReduce
		intermediate[idx] = append(intermediate[idx], kv)
	}

	// write to files
	for i, kvs := range intermediate {
		temp, err := ioutil.TempFile(".", "")
		if err != nil {
			log.Fatalf("failed to create temp file: %v", err)
		}
		enc := json.NewEncoder(temp)
		for _, kv := range kvs {
			enc.Encode(&kv)
		}
		// atomically rename the file as mr-workerX-Y,where X is taskId, and Y is reduceId
		temp.Close()
		if err := os.Rename(temp.Name(), fmt.Sprintf("mr-worker%d-%d", task.ID, i)); err != nil {
			panic(err)
		}
	}

}

func reduceTask(task Task, reducef func(string, []string) string) {
	fmt.Printf("start reduceTask %v with output filename %v\n", task.ID, task.Filename)

	kvs := make([]KeyValue, 0)
	// read all mr-*-ReduceID files
	path, _ := os.Getwd()
	files, err := filepath.Glob(fmt.Sprintf(path + "/mr-*-%d", task.ID))
	if err != nil {
		panic(err)
	}
	for _, file := range files {
		in, err := os.Open(file)
		if err != nil {
			panic(err)
		}

		dec := json.NewDecoder(in)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kvs = append(kvs, kv)
		}
		in.Close()
	}

	sort.Sort(ByKey(kvs))

	ofile, _ := ioutil.TempFile(".", "")

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(kvs) {
		j := i + 1
		for j < len(kvs) && kvs[j].Key == kvs[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kvs[k].Value)
		}
		output := reducef(kvs[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kvs[i].Key, output)

		i = j
	}
	ofile.Close()
	os.Rename(ofile.Name(), task.Filename)
}


//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

func CallGetTask() TaskReply {

	// declare an argument structure.
	args := TaskArgs{}

	// declare a reply structure.
	reply := TaskReply{}

	// send the RPC request, wait for the reply.
	if success := call("Master.GetTask", &args, &reply); !success {
		fmt.Println("failed to call rpc, maybe master has finished its job")
		os.Exit(0)
	}
	fmt.Printf("get task reply %+v\n", reply)
	return reply
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
