package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// lab1 add
// for sorting by key.
type ByKey []KeyValue

// lab1 add
// for sorting by key.
// 为ByKey类型提供排序需要的接口实现
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// main/mrworker.go calls this function.
//
// lab1 modify
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	// 启动Worker
	for {
		// 向master发起RPC请求获取一个任务
		task := getTask()
		switch task.TaskState {
		case Map:
			mapper(&task, mapf)
		case Reduce:
			reducer(&task, reducef)
		case Wait:
			time.Sleep(5 * time.Second)
		case Exit:
			return
		}
	}
	// uncomment to send the Example RPC to the master.
	// CallExample()

}

// lab1 add
// 向master发起RPC请求获取一个任务
// 参考callExample()
func getTask() Task {
	args := ExampleArgs{}
	reply := Task{}
	call("Master.AssignTask", &args, &reply)
	return reply
}

// lab1 add
// map task处理
// mapf:自定义的map处理函数
func mapper(task *Task, mapf func(string, string) []KeyValue) {
	//根据task.Input文件名读取content
	content, err := ioutil.ReadFile(task.Input)
	if err != nil {
		log.Fatal("Failed to read file:"+task.Input, err)
	}
	// 将content交给mapf处理
	// 内存缓存结果
	intermediates := mapf(task.Input, string(content))

	// 缓存后的结果写回本地磁盘,切割成N份 (NReducer)
	// 切分方式:根据key做hash
	buffer := make([][]KeyValue, task.NReducer)
	for _, intermediate := range intermediates {
		slot := ihash(intermediate.Key) % task.NReducer
		buffer[slot] = append(buffer[slot], intermediate)
	}
	mapOutput := make([]string, 0)
	for i := 0; i < task.NReducer; i++ {
		mapOutput = append(mapOutput, writeToLocalFile(task.TaskNumber, i, &buffer[i]))
	}
	// NReduer个文件的位置发送给master,由master协调reducer处理
	task.Intermediates = mapOutput
	TaskCompleted(task)
}

// lab1 add
// reduce task处理
// reducef:自定义的map处理函数
func reducer(task *Task, reducef func(string, []string) string) {
	// 根据task.Integermediates读取mapper处理后的中间文件内容KeyValue
	// 一个中间文件的所有KV对：intermediate
	intermediate := *readToLocalFile(task.Intermediates)
	// 根据KV排序
	sort.Sort(ByKey(intermediate))
	dir, _ := os.Getwd()
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Failed to create temp file", err)
	}
	// 参考mrsequential.go
	i := 0
	for i < len(intermediate) {
		// 相同的key放在一起分组combine
		// 按照Key排序后找到一段[i,j), intermediate[i-j).Key相等
		j := i + 1
		//
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		// 将读取的内容交给reducef处理
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	tempFile.Close()
	oname := fmt.Sprintf("mr-out-%d", task.TaskNumber)
	os.Rename(tempFile.Name(), oname)
	task.Output = oname
	TaskCompleted(task)
}

// lab1 add
// 写入本地磁盘文件辅助函数,用于mapper中处理后写入NReducer个本地文件
// 返回文件的路径
func writeToLocalFile(x int, y int, kvs *[]KeyValue) string {
	// 获取pwd
	dir, _ := os.Getwd()
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Failed to create temp file", err)
	}
	enc := json.NewEncoder(tempFile)
	for _, kv := range *kvs {
		if err := enc.Encode(&kv); err != nil {
			log.Fatal("Failed to write kv pair", err)
		}
	}
	tempFile.Close()
	outputName := fmt.Sprintf("mr-%d-%d", x, y)
	// 修改已经写入磁盘的临时文件的名称
	os.Rename(tempFile.Name(), outputName)
	// dir/outputName
	return filepath.Join(dir, outputName)
}

// lab1 add
// 读取本地磁盘文件辅助函数,用于reducer中读取mapper处理后写的NReducer个本地文件
// 返回KeyValue数组
func readToLocalFile(files []string) *[]KeyValue {
	kva := []KeyValue{}
	for _, filepath := range files {
		file, err := os.Open(filepath)
		if err != nil {
			log.Fatal("Failed to open file "+filepath, err)
		}
		desc := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := desc.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	return &kva
}

// lab1 add
// 通过RPC,把task完成的信息发送给master
func TaskCompleted(task *Task) {
	reply := ExampleReply{}
	call("Master.TaskCompleted", task, &reply)
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
