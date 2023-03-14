package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	//Your worker implementation here.
	for {
		task := getTask()

		switch task.State {
		case Map:
			maper(&task, mapf)
		case Reduce:
			reducer(&task, reducef)
		case Wait:
			time.Sleep(5 * time.Second)
		case Exit:
			return
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	//CallExample()

}

func maper(task *Task, mapf func(string, string) []KeyValue) {
	content, err := ioutil.ReadFile(task.Input)
	if err != nil {
		log.Fatal("Failed to read map file:"+task.Input, err)
	}
	//map函数切割成<k,v>的数组 keyvalue[]
	intermediates := mapf(task.Input, string(content))
	//把map的中间结果切成ReduceN份,缓存在buffer中
	buffer := make([][]KeyValue, task.NReduce)
	for _, intermediate := range intermediates {
		slot := ihash(intermediate.Key) % task.NReduce
		buffer[slot] = append(buffer[slot], intermediate)
	}
	fmt.Println(buffer[0])
	mapOutput := make([]string, 0)
	for i := 0; i < task.NReduce; i++ {
		//buffer写入磁盘并记录中间文件的位置
		mapOutput = append(mapOutput, writeToLocalFile(task.TaskId /*map*/, i /*reduce*/, &buffer[i]))
	}
	////R个文件的位置发送给master
	task.Intermediate = mapOutput
	//for i, s := range task.Intermediate {
	//	fmt.Println(i, ":", s)
	//}
	TaskCompleted(task)
}

func writeToLocalFile(id int, reduceId int, kvs *[]KeyValue) string {
	dir, _ := os.Getwd()
	tempfile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Failed to create file", err)
	}
	enc := json.NewEncoder(tempfile)
	for _, kv := range *kvs {
		if err := enc.Encode(&kv); err != nil {
			log.Fatal("Failed to write kv pair", err)
		}
	}
	tempfile.Close()
	outputName := fmt.Sprintf("mr-%d-%d", id, reduceId)
	os.Rename(tempfile.Name(), outputName)
	return filepath.Join(dir, outputName)
}

func reducer(task *Task, reducef func(string, []string) string) {
	intermediate := *readFromLocalFile(task.Intermediate)
	//fmt.Println("reduce+++", intermediate)
	sort.Sort(ByKey(intermediate))
	dir, _ := os.Getwd()
	tempfile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Failed to create file", err)
	}
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tempfile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	tempfile.Close()
	oname := fmt.Sprintf("mr-out-%d", task.TaskId)
	os.Rename(tempfile.Name(), oname)
	task.Output = oname
	TaskCompleted(task)
}

func readFromLocalFile(files []string) *[]KeyValue {
	kva := []KeyValue{}
	for _, filepath := range files {
		file, err := os.Open(filepath)
		if err != nil {
			log.Fatal("Failed to open file"+filepath, err)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}
	return &kva
}

func getTask() Task {
	args := ExampleArgs{}
	reply := Task{}
	ok := call("Coordinator.AssignTask", &args, &reply)
	if ok {
		fmt.Printf("RPC Coordinator.AssignTask successed\n")
	} else {
		fmt.Printf("RPC Coordinator.AssignTask failed\n")
	}
	return reply
}

func TaskCompleted(task *Task) {
	reply := ExampleReply{}
	for i, s := range task.Intermediate {
		fmt.Println("TaskCom", i, ":", s)
	}
	ok := call("Coordinator.TaskCompleted", task, &reply)
	if ok {
		fmt.Printf("RPC Coordinator.TaskCompleted successed\n")
	} else {
		fmt.Printf("RPC Coordinator.TaskCompleted failed\n")
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	//args := ExampleArgs{}
	args := Task{Intermediate: make([]string, 2), Input: "000"}
	args.Intermediate[0] = "123"
	args.Intermediate[1] = "212"
	// fill in the argument(s).
	//args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	//sockname := coordinatorSock()
	//c, err := rpc.DialHTTP("unix", sockname)
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
