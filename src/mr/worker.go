package mr

import (
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	id := os.Getegid()
	// log.Printf("worker %d start working", id)
	lastTaskId := -1
	lastTaskType := ""

loop:
	for {
		args := ApplyArgs{
			WorkerID:     id,
			LastTaskType: lastTaskType,
			LastTaskID:   lastTaskId,
		}

		reply := ReplyArgs{}

		ok := call("Coordinator.ApplyForTask", &args, &reply)
		if !ok {
			fmt.Printf("call failed!\n")
			continue
		}
		// log.Printf("reply: %v", reply)
		lastTaskId = reply.TaskId
		lastTaskType = reply.TaskType
		switch reply.TaskType {
		case "":
			// log.Println("finished")
			break loop
		case MAP:
			// log.Printf("worker %d get map task %d", id, reply.TaskId)
			doMapTask(id, reply.TaskId, reply.InputFile, reply.ReduceNum, mapf)
		case REDUCE:
			// log.Printf("worker %d get reduce task %d", id, reply.TaskId)
			doReduceTask(id, reply.TaskId, reply.MapNum, reducef)
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func doMapTask(id int, taskId int, filename string, reduceNum int, mapf func(string, string) []KeyValue) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("%s 文件打开失败! ", filename)
		return
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("%s 文件内容读取失败! ", filename)
	}
	file.Close()
	kvList := mapf(filename, string(content)) // kv list
	hashedKvList := make(map[int]ByKey)
	for _, kv := range kvList {
		hashedKey := ihash(kv.Key) % reduceNum
		hashedKvList[hashedKey] = append(hashedKvList[hashedKey], kv)
	}

	for i := 0; i < reduceNum; i++ {
		outFile, err := os.Create(tmpMapResult(id, taskId, i))
		if err != nil {
			log.Fatalf("can not create output file: %e", err)
			return
		}
		for _, kv := range hashedKvList[i] {
			fmt.Fprintf(outFile, "%v\t%v\n", kv.Key, kv.Value)
		}
		outFile.Close()
	}
	// log.Printf("worker %d finished map task\n", id)
}

func doReduceTask(id int, taskId int, mapNum int, reducef func(string, []string) string) {
	var kvList ByKey
	var lines []string
	for i := 0; i < mapNum; i++ {
		mapOutFile := finalMapResult(i, taskId)
		file, err := os.Open(mapOutFile)
		if err != nil {
			log.Fatalf("can not open output file %s: %e", mapOutFile, err)
			return
		}
		content, err := io.ReadAll(file)
		if err != nil {
			log.Fatalf("file read failed %s: %e", mapOutFile, err)
			return
		}
		lines = append(lines, strings.Split(string(content), "\n")...)
	}
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		split := strings.Split(line, "\t")
		kvList = append(kvList, KeyValue{Key: split[0], Value: split[1]})
	}
	sort.Sort(kvList)
	outputFile := tmpReduceResult(id, taskId)
	file, err := os.Create(outputFile)
	if err != nil {
		log.Fatalf("can not create output file: %e", err)
		return
	}

	for i := 0; i < len(kvList); {
		j := i + 1
		key := kvList[i].Key
		var values []string
		for j < len(kvList) && kvList[j].Key == key {
			j++
		}
		for k := i; k < j; k++ {
			values = append(values, kvList[k].Value)
		}
		res := reducef(key, values)
		fmt.Fprintf(file, "%v %v\n", key, res)
		i = j
	}
	file.Close()
	// log.Printf("worker %d finished reduce task", id)
}
