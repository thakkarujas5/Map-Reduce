package main

import (
	"fmt"
	"log"
	"mr/shared"
	"net/rpc"
	"time"
)

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

func (w *MapWorker) run() {

	for {
		task, ok := getMapTask(w.client)

		if !ok {
			//	fmt.Println("No task found")
			time.Sleep(100 * time.Millisecond)
			continue
		}

		if task.Type == shared.MapTask {
			kv := shared.Map(task.File)
			fmt.Println(kv)
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
