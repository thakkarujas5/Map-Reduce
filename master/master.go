package master

import (
	"fmt"
	"os"
	"sync"

	"mr/shared"
)

type Master struct {
	mu          sync.Mutex
	mapTasks    []shared.Task
	reduceTasks []shared.Task
	nMap        int
	nReduce     int
}

func MakeMaster(files []string, reduceTasks int) *Master {

	master := Master{}

	mapTasks := len(files)
	master.nMap = mapTasks
	master.nReduce = reduceTasks
	master.mapTasks = make([]shared.Task, 0, mapTasks)
	master.reduceTasks = make([]shared.Task, 0, reduceTasks)

}
func main() {

	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "No Input Files provided ")
		os.Exit(1)
	}

	x := shared.Map("example.txt")
	fmt.Print(x)
}
