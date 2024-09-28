package main

import (
	"fmt"
	"log"
	"net/rpc"
	"time"
)

type MapWorker struct {
	client *rpc.Client
}

func (w *MapWorker) run() {
	for {
		// pseudocode for map worker
		// task, err := w.GetTask()
		// if err != nil {
		// 	log.Printf("Error getting task: %v", err)
		// 	return
		// }

		// if task.TaskType == "map" {
		// 	w.doMapTask(task)
		// } else if task.TaskType == "done" {
		// 	fmt.Println("All tasks completed. Worker exiting.")
		// 	return
		// }
		fmt.Println("Map Worker running")
		time.Sleep(100 * time.Millisecond)
	}
}

func main() {
	client, err := rpc.DialHTTP("tcp", "localhost:1234")
	if err != nil {
		log.Fatal("dialing:", err)
	}

	worker := &MapWorker{client: client}
	worker.run()
}
