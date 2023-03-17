package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// Worker所处工作状态
const (
	Map    State = iota //0
	Reduce              //1
	Exit                //2
	Wait                //3
)

// lab1 add
// 任务状态阶段枚举常量
const (
	Idle       MasterTaskStatus = iota //0
	InProgress                         //1
	Completed                          //2
)

// lab1 add
type State int //任务阶段

// lab1 add
type MasterTaskStatus int //Master任务状态

// lab1 add
type Task struct {
	Input         string
	TaskState     State
	NReducer      int
	TaskNumber    int
	Intermediates []string
	Output        string
}

// lab1 add
type MasterTask struct {
	TaskStatus    MasterTaskStatus
	StartTime     time.Time
	TaskReference *Task
}

// lab1 modify
type Master struct {
	// Your definitions here.
	TaskQueue     chan *Task          //任务等待队列,保存等待执行的TaskNumber
	TaskMeta      map[int]*MasterTask //当前所有Task的信息<TaskNumber,MasterTask引用>
	MasterPhase   State               //Master的状态
	NReduce       int
	InputFiles    []string
	Intermediates [][]string //Map任务产生的R个中间文件的信息
}

// lab1 add
var mu sync.Mutex

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
// lab1 modify
func MakeMaster(files []string, nReduce int) *Master {

	// Your code here.
	// lab1 modify
	m := Master{
		TaskQueue:     make(chan *Task, max(nReduce, len(files))),
		TaskMeta:      make(map[int]*MasterTask),
		MasterPhase:   Map,
		NReduce:       nReduce,
		InputFiles:    files,
		Intermediates: make([][]string, nReduce),
	}

	//将输入文件切分成大小在16-64MB之间的文件
	//创建Map任务
	m.createMapTask()

	//在一组多个机器上启动用户程序
	//一个程序成为master,其他成为worker
	//这里就是启动master服务器就行了，
	//拥有master代码的就是master，别的发RPC过来的都是worker
	m.server()

	// 启动一个goroutine 检查超时的任务
	go m.catchTimeOut()

	return &m
}

// lab1 add
// Master m作为接收者的方法,创建Map任务
func (m *Master) createMapTask() {
	// 根据m记录的filename,每个文件对应创建一个map task
	for idx, filename := range m.InputFiles {
		taskMeta := Task{
			Input:      filename,
			TaskState:  Map,
			NReducer:   m.NReduce,
			TaskNumber: idx,
		}
		// 将创建的taskMeta写入等待执行任务的队列通道
		m.TaskQueue <- &taskMeta
		// Master的Meta哈希表中添加 <TaskNumber,taskMeta>的KV,
		// 用于根据Number找到该task
		m.TaskMeta[idx] = &MasterTask{
			TaskStatus:    Idle,
			TaskReference: &taskMeta,
		}
	}
}

// lab1 add
// // Master m作为接收者的方法,创建reduce任务
func (m *Master) createReduceTask() {
	m.TaskMeta = make(map[int]*MasterTask)
	for idx, files := range m.Intermediates {
		taskMeta := Task{
			TaskState:     Reduce,
			NReducer:      m.NReduce,
			TaskNumber:    idx,
			Intermediates: files,
		}
		m.TaskQueue <- &taskMeta
		m.TaskMeta[idx] = &MasterTask{
			TaskStatus:    Idle,
			TaskReference: &taskMeta,
		}
	}
}

// lab1 add
// Master m用于检查记录的所有Task中有无超时的task
func (m *Master) catchTimeOut() {
	for {
		time.Sleep(5 * time.Second)
		mu.Lock()
		if m.MasterPhase == Exit {
			mu.Unlock()
			return
		}
		for _, masterTask := range m.TaskMeta {
			// 如果当前任务在执行，且执行时间超过了10s
			if masterTask.TaskStatus == InProgress && time.Now().Sub(masterTask.StartTime) > 10*time.Second {
				// 任务重新进入等待队列
				m.TaskQueue <- masterTask.TaskReference
				// 重置任务状态
				masterTask.TaskStatus = Idle
			}
		}
		mu.Unlock()

	}
}

// lab1 add
// master等待worker发起rpc getTask, 使用本方法给worker响应布置的task
func (m *Master) AssignTask(args *ExampleArgs, reply *Task) error {
	// 先查看master自己有没有等待执行的task
	mu.Lock()
	defer mu.Unlock()
	// 有等待执行的task
	if len(m.TaskQueue) > 0 {
		// 从队列中发出去一个
		*reply = *<-m.TaskQueue
		// 更新元数据哈希表,这个任务开始执行了,修改状态和开始时间
		m.TaskMeta[reply.TaskNumber].TaskStatus = InProgress
		m.TaskMeta[reply.TaskNumber].StartTime = time.Now()
	} else if m.MasterPhase == Exit {
		// Master已经退出
		// 返回无效任务
		*reply = Task{TaskState: Exit}
	} else {
		// Master中没有待执行的任务
		// 让请求的Worker等待
		*reply = Task{TaskState: Wait}
	}
	return nil
}

// lab1 add
// 收到worker.TaskCompleted完成的通知RPC信号后
// 更新task状态,处理结果
func (m *Master) TaskCompleted(task *Task, reply *ExampleReply) error {
	// 更新task状态
	mu.Lock()
	defer mu.Unlock()
	// worker写在同一个磁盘文件上,对于重复的结果要丢弃
	// master记录中该task已经完成
	// 或者该task的工作状态和master的状态不一致
	if task.TaskState != m.MasterPhase || m.TaskMeta[task.TaskNumber].TaskStatus == Completed {
		return nil
	}
	// 该task标记为已完成
	m.TaskMeta[task.TaskNumber].TaskStatus = Completed
	go m.processTaskResult(task)
	return nil
}

// lab1 add
func (m *Master) processTaskResult(task *Task) {
	mu.Lock()
	defer mu.Unlock()
	switch task.TaskState {
	case Map:
		//收集intermediate信息
		for reduceTaskId, filePath := range task.Intermediates {
			m.Intermediates[reduceTaskId] = append(m.Intermediates[reduceTaskId], filePath)
		}
		// master获得所有的map task done RPC后,整体进入Reduce阶段
		if m.allTaskDone() {
			m.createReduceTask()
			// 整体进入Reduce阶段
			m.MasterPhase = Reduce
		}
	case Reduce:
		// master获得所有的reduce task done RPC后,整体进入exit阶段
		if m.allTaskDone() {
			m.MasterPhase = Exit
		}
	}
}

// lab1 add
func (m *Master) allTaskDone() bool {
	for _, task := range m.TaskMeta {
		if task.TaskStatus != Completed {
			return false
		}
	}
	return true
}

// lab1 add
func max(a, b int) int {
	if a >= b {
		return a
	}
	return b
}
