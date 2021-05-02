package main

import (
	"fmt"
	"io/ioutil"
	"os"
)

func main() {
	file, _ := ioutil.TempFile(".", "")
	fmt.Println(file.Name())
	file.Close()
	os.Rename(file.Name(), "abc.test")
}
