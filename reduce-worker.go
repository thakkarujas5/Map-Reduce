package main

import (
	"fmt"
	"log"
	"mr/shared"
	"net/rpc"
	"os"
	"sort"
	"time"
)

type ReduceWorker struct {
	client *rpc.Client
}

func getReduceTask(client *rpc.Client) (shared.Task, bool) {
	args := shared.GetReduceTaskArgs{}
	reply := shared.GetReduceTaskReply{}
	err := client.Call("Master.GetReduceTask", &args, &reply)
	if err != nil {
		fmt.Print(err)
	}
	return reply.Task, reply.Ok
}

func writeReduceOutput(kvMap map[string][]string, reduceId int) {

	keys := make([]string, 0, len(kvMap))
	for k := range kvMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	// Create temp file
	filePath := fmt.Sprintf("%v/mr-out-%v-%v", shared.TempDir, reduceId, os.Getpid())
	file, err := os.Create(filePath)
	checkError(err, "Cannot create file %v\n", filePath, file)

	// Call reduce and write to temp file
	for _, k := range keys {

		v := shared.PerformReduce(kvMap[k])
		_, err := fmt.Fprintf(file, "%v %v\n", k, shared.PerformReduce(kvMap[k]))
		checkError(err, "Cannot write mr output (%v, %v) to file", k, v)
	}

	// // atomically rename temp files to ensure no one observes partial files
	// file.Close()
	//fmt.Println("%v/mr-out-%v", "out", reduceId)
	newPath := fmt.Sprintf("mr-out-%v", reduceId)
	err = os.Rename(filePath, newPath)
	checkError(err, "Cannot rename file %v\n", filePath)
}

func checkError(err error, format string, v ...interface{}) {
	if err != nil {
		log.Fatalf(format, v)
	}
}

func reportReduceTask(client *rpc.Client, task shared.Task) {
	args := shared.ReportReduceTaskArgs{Task: task}
	reply := shared.ReportReduceTaskReply{}

	err := client.Call("Master.ReportReduceTask", &args, &reply)
	if err != nil {
		fmt.Print(err)
	}
}

func (w *ReduceWorker) run() {

	for {
		task, ok := getReduceTask(w.client)

		if task.Type == shared.ExitTask {
			fmt.Println("All reduce tasks finished")
			return
		}

		kvMap := shared.Reduce(task.Index)

		writeReduceOutput(kvMap, task.Index)
		reportReduceTask(w.client, task)
		time.Sleep(2 * time.Second)
		if !ok {
			time.Sleep(100 * time.Millisecond)
			continue
		}
	}
}

func main() {

	client, err := rpc.DialHTTP("tcp", "localhost:1234")
	if err != nil {
		log.Fatal("dialing:", err)
	}

	worker := &ReduceWorker{client: client}
	worker.run()
}
