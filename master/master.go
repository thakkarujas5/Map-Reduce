package master

import (
	"fmt"
	"log"
	"mr/shared"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Master struct {
	mu          sync.Mutex
	mapTasks    []shared.Task
	reduceTasks []shared.Task
	nMap        int
	nReduce     int
}

func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()

	listener, err := net.Listen("tcp", ":1234")
	if err != nil {
		log.Fatal("Listen error:", err)
	}

	log.Println("RPC server listening on port 1234")
	http.Serve(listener, nil)
}

func (m *Master) ReportTask(args *shared.ReportMapTaskArgs, reply *shared.ReportMapTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for i, t := range m.mapTasks {
		if t.Index == args.Task.Index {
			m.mapTasks[i].Status = shared.Finished
			return nil
		}
	}

	return fmt.Errorf("task with index %d not found", args.Task.Index)
}

func (m *Master) GetReduceCount(args *shared.GetReduceCountArgs, reply *shared.GetReduceCountReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	reply.Count = len(m.reduceTasks)
	return nil
}

func (m *Master) GetTask(args *shared.GetMapTaskArgs, reply *shared.GetMapTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, task := range m.mapTasks {
		if task.Status == shared.NotStarted {
			m.mapTasks[i].Status = shared.InProgress
			reply.Task = m.mapTasks[i]
			reply.Ok = true
			return nil
		}
	}

	// } else if taskType == shared.ReduceTask {
	// 	// Check if all map tasks are completed before assigning reduce tasks
	// 	if !m.allMapTasksCompleted() {
	// 		return shared.Task{}, errors.New("map tasks not completed yet")
	// 	}

	// 	// Check for available reduce tasks
	// 	for i, task := range m.reduceTasks {
	// 		if task.Status == shared.NotStarted {
	// 			m.reduceTasks[i].Status = shared.InProgress
	// 			return m.reduceTasks[i], nil
	// 		}
	// 	}
	// }

	return nil
}

func MakeMaster(files []string, reduceTasks int) *Master {

	master := Master{}

	mapTasks := len(files)
	master.nMap = mapTasks
	master.nReduce = reduceTasks
	master.mapTasks = make([]shared.Task, 0, mapTasks)
	master.reduceTasks = make([]shared.Task, 0, reduceTasks)

	for i := 0; i < mapTasks; i++ {
		mTask := shared.Task{Type: shared.MapTask, Status: shared.NotStarted, Index: i, File: files[i], WorkerId: -1}
		master.mapTasks = append(master.mapTasks, mTask)
	}

	for i := 0; i < reduceTasks; i++ {
		rTask := shared.Task{Type: shared.ReduceTask, Status: shared.NotStarted, Index: i, File: "", WorkerId: -1}
		master.reduceTasks = append(master.reduceTasks, rTask)
	}

	err := os.Mkdir(shared.TempDir, 0755)
	if err != nil {
		log.Fatalf("Cannot create temp directory %v\n", shared.TempDir)
	}
	fmt.Println(master.mapTasks)
	master.server()

	return &master
}
