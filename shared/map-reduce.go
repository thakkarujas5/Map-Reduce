package shared

import (
	"bufio"
	"fmt"
	"mr/worker"
	"os"
	"strconv"
)

type TaskStatus int
type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	NoTask
	ExitTask
)

const (
	NotStarted TaskStatus = iota
	InProgress
	Executing
	Finished
)

type Task struct {
	Type     TaskType
	Status   TaskStatus
	Index    int
	File     string
	WorkerId int
}

func Map(fileName string) []worker.KeyValue {

	file, err := os.Open(fileName)

	if err != nil {
		fmt.Printf("Error reading file: %v\n", err)
	}

	defer file.Close()

	kva := []worker.KeyValue{}

	scanner := bufio.NewScanner(file)

	scanner.Split(bufio.ScanWords)

	for scanner.Scan() {

		line := scanner.Text()
		kv := worker.KeyValue{Key: line, Value: "1"}
		kva = append(kva, kv)
	}

	return kva
}

func Reduce(values []string) string {
	return strconv.Itoa(len(values))
}
