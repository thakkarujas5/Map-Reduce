package shared

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"mr/worker"
	"os"
	"path/filepath"
	"strconv"
)

const TempDir = "./tmp"

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

func Reduce(index int) map[string][]string {
	files, err := filepath.Glob(fmt.Sprintf("%v/mr-%v-%v", TempDir, "*", index))
	if err != nil {
		fmt.Printf("Error reading file: %v\n", err)
	}

	kvMap := make(map[string][]string)
	var kv worker.KeyValue

	for _, filePath := range files {
		file, err := os.Open(filePath)
		checkError(err, "Cannot open file %v\n", filePath)

		dec := json.NewDecoder(file)
		for dec.More() {
			err = dec.Decode(&kv)
			checkError(err, "Cannot decode from file %v\n", filePath)
			kvMap[kv.Key] = append(kvMap[kv.Key], kv.Value)
		}
	}

	return kvMap
}

func PerformReduce(reduceArray []string) string {
	return strconv.Itoa(len(reduceArray))
}

type GetMapTaskArgs struct {
}

type GetMapTaskReply struct {
	Task Task
	Ok   bool
}

type ReportMapTaskArgs struct {
	Task Task
}

type ReportMapTaskReply struct {
}

type GetReduceCountArgs struct {
}

type GetReduceCountReply struct {
	Count int
}

type GetReduceTaskArgs struct {
}

type GetReduceTaskReply struct {
	Task Task
	Ok   bool
}

func checkError(err error, format string, v ...interface{}) {
	if err != nil {
		log.Fatalf(format, v)
	}
}
