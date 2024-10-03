package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"mr/shared"
	"mr/worker"
	"net/rpc"
	"os"
	"time"
)

var nReduce int

type MapWorker struct {
	client *rpc.Client
}

func getMapTask(client *rpc.Client) (shared.Task, bool) {
	args := shared.GetMapTaskArgs{}
	reply := shared.GetMapTaskReply{}

	err := client.Call("Master.GetTask", &args, &reply)

	if err != nil {
		fmt.Print(err)
	}

	return reply.Task, reply.Ok
}

func reportMapTask(client *rpc.Client, task shared.Task) {
	args := shared.ReportMapTaskArgs{Task: task}
	reply := shared.ReportMapTaskReply{}

	err := client.Call("Master.ReportTask", &args, &reply)
	if err != nil {
		fmt.Print(err)
	}
}

func getReduceCount(client *rpc.Client) int {
	args := shared.GetReduceCountArgs{}
	reply := shared.GetReduceCountReply{}

	err := client.Call("Master.GetReduceCount", &args, &reply)
	if err != nil {
		fmt.Print(err)
	}

	return reply.Count
}

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func checkError(err error, format string, v ...interface{}) {
	if err != nil {
		log.Fatalf(format, v)
	}
}

func writeMapOutput(kva []worker.KeyValue, mapId int) {
	// use io buffers to reduce disk I/O, which greatly improves
	// performance when running in containers with mounted volumes
	prefix := fmt.Sprintf("%v/mr-%v", shared.TempDir, mapId)
	//fmt.Println("prefix:", prefix)
	files := make([]*os.File, 0, nReduce)
	buffers := make([]*bufio.Writer, 0, nReduce)
	encoders := make([]*json.Encoder, 0, nReduce)

	// create temp files, use pid to uniquely identify this worker
	for i := 0; i < nReduce; i++ {
		filePath := fmt.Sprintf("%v-%v-%v", prefix, i, os.Getpid())
		file, err := os.Create(filePath)
		checkError(err, "Cannot create file %v\n", filePath)
		buf := bufio.NewWriter(file)
		files = append(files, file)
		buffers = append(buffers, buf)
		encoders = append(encoders, json.NewEncoder(buf))
	}

	// write map outputs to temp files
	for _, kv := range kva {
		idx := ihash(kv.Key) % nReduce
		err := encoders[idx].Encode(&kv)
		checkError(err, "Cannot encode %v to file\n", kv)
	}

	// flush file buffer to disk
	for i, buf := range buffers {
		err := buf.Flush()
		checkError(err, "Cannot flush buffer for file: %v\n", files[i].Name())
	}

	//atomically rename temp files to ensure no one observes partial files
	for i, file := range files {
		file.Close()
		newPath := fmt.Sprintf("%v-%v", prefix, i)
		err := os.Rename(file.Name(), newPath)
		checkError(err, "Cannot rename file %v\n", file.Name())
	}
}

func (w *MapWorker) run() {
	reduceCount := getReduceCount(w.client)
	nReduce = reduceCount
	fmt.Println("reduceCount: ", reduceCount)

	for {
		task, ok := getMapTask(w.client)

		if !ok {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		if task.Type == shared.MapTask {
			kv := shared.Map(task.File)
			writeMapOutput(kv, task.Index)
			reportMapTask(w.client, task)
		}
	}

	//fmt.Println(task, ok)
	// for {
	// 	pseudocode for map worker
	// 	task, err := w.GetTask()
	// 	if err != nil {
	// 		log.Printf("Error getting task: %v", err)
	// 		return
	// 	}

	// 	if task.TaskType == "map" {
	// 		w.doMapTask(task)
	// 	} else if task.TaskType == "done" {
	// 		fmt.Println("All tasks completed. Worker exiting.")
	// 		return
	// 	}
	// 	fmt.Println("Map Worker running")
	// 	time.Sleep(100 * time.Millisecond)
	// }
}

func main() {
	client, err := rpc.DialHTTP("tcp", "localhost:1234")
	if err != nil {
		log.Fatal("dialing:", err)
	}

	worker := &MapWorker{client: client}
	worker.run()
}
