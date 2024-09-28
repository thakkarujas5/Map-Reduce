package master

import (
	"log"
	"mr/shared"
	"net"
	"net/http"
	"net/rpc"
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

	master.server()

	return &master
}

func (m *Master) GetReduceCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return len(m.reduceTasks)
}
