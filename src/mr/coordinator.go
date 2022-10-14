package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import (
	"sync"
	"time"
	"io/ioutil"
	"strconv"
	"strings"
)

type Coordinator struct {
	// Your definitions here.
	WorkingPhase 	Phase
	TaskId			int
	ReducerNumber	int
	MapTaskChan		chan *Task
	ReduceTaskChan	chan *Task
	TaskMeta		TaskMeta
	Files			[]string
}

type TaskMeta struct {
	MetaMap map[int]*TaskInfo
}
type TaskInfo struct{
	TaskState	TaskState
	StartTime	time.Time
	TaskAdd		*Task
}

var mu sync.Mutex
//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		WorkingPhase	:MappingPhase,
		ReducerNumber	:nReduce,
		MapTaskChan		:make(chan *Task,len(files)),
		ReduceTaskChan	:make(chan *Task,nReduce),
		TaskMeta		:TaskMeta{
			MetaMap		:make(map[int]*TaskInfo,len(files)+nReduce),
		},
		Files			:files,
	}
	c.MakeMapTasks(files)
	// Your code here.
	go c.CrashDetect()
	c.server()
	return &c
}
func (c *Coordinator) CrashDetect(){
	for{
		time.Sleep(time.Second * 2)
		mu.Lock()
		if c.WorkingPhase==AllDone{
			mu.Unlock()
			break
		}
		for _,v := range c.TaskMeta.MetaMap{
			if v.TaskState == Working && time.Since(v.StartTime)>9*time.Second{
				// LogPrint("Task %d is Crash %v\n",v.TaskAdd.TaskId,v.TaskAdd)
				switch v.TaskAdd.TaskType {
				case MapTask:
					c.MapTaskChan <- v.TaskAdd
					v.TaskState = Waitting
				case ReduceTask:
					c.ReduceTaskChan <- v.TaskAdd
					v.TaskState = Waitting

				}
			}
		}
		mu.Unlock()
	}
	
}
func (c *Coordinator) MakeMapTasks(files []string)  {
	for _,file := range files{
		id := c.GenerateTaskId()
		task :=Task{
			TaskType 		:MapTask,
			TaskId			:id,
			ReducerNumber	:c.ReducerNumber,
			Files			:[]string{file},
		}
		taskInfo := TaskInfo{
			TaskState		:Waitting,
			TaskAdd			:&task,
		}
		c.TaskMeta.addTaskInfo(&taskInfo)
		c.MapTaskChan <- &task
	}
}
func (c *Coordinator) MakeReduceTasks()  {
	for i:=0 ;i<c.ReducerNumber;i++{
		id := c.GenerateTaskId()
		task :=Task{
			TaskType 		:ReduceTask,
			TaskId			:id,
			ReducerNumber	:c.ReducerNumber,
			Files			:c.getReducefile(i),
		}
		taskInfo := TaskInfo{
			TaskState		:Waitting,
			TaskAdd			:&task,
		}
		c.TaskMeta.addTaskInfo(&taskInfo)
		c.ReduceTaskChan <- &task
	}
}
func (taskMeta *TaskMeta) addTaskInfo(taskInfo *TaskInfo){
	if taskMeta.MetaMap[taskInfo.TaskAdd.TaskId]==nil{
		taskMeta.MetaMap[taskInfo.TaskAdd.TaskId]=taskInfo
	}
}
func (c *Coordinator) getReducefile(i int) []string{
	var res []string
	dir,_ := os.Getwd()
	files,_ :=ioutil.ReadDir(dir)
	for _,file := range files{
		if strings.HasPrefix(file.Name(), "mr-tmp") && strings.HasSuffix(file.Name(), strconv.Itoa(i)) {
			res = append(res, file.Name())
		}
	}
	return res
}
func (c *Coordinator) GenerateTaskId() int{
	res := c.TaskId
	c.TaskId++
	return res
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *TaskMeta) checkTaskDone() bool {
	res := false
	var (
		mapdone		=0
		mapundone	=0
		reducedone	=0
		reduceundone=0
	)
	for _,v := range m.MetaMap{
		if v.TaskAdd.TaskType == MapTask{
			if v.TaskState == Done{
				mapdone++
			}else {
				mapundone++
			}
		}else if v.TaskAdd.TaskType == ReduceTask{
			if v.TaskState == Done{
				reducedone++
			}else {
				reduceundone++
			}
		}
	}
	// LogPrint("map %d %d , reduce %d %d\n",mapdone,mapundone,reducedone,reduceundone)
	if mapdone>0&&mapundone==0&&reducedone==0&&reduceundone==0{
		//mapPhaseDone
		res = true
	}else if reducedone>0&&reduceundone==0{
		//reducePhaseDone
		res = true
	}
	return res
}
func (c *Coordinator) ToNextState(){
	if c.WorkingPhase == MappingPhase{
		c.WorkingPhase = ReducingPhase
		c.MakeReduceTasks()
	}else{
		c.WorkingPhase = AllDone
	}
}
func (c *Coordinator) PullTask(args *TaskArgs, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()
	switch c.WorkingPhase{
	case MappingPhase:{
		// LogPrint("mapTask channel length %d\n",len(c.MapTaskChan))
		if len(c.MapTaskChan)>0{
			*reply = *<-c.MapTaskChan
			c.TaskMeta.ChangeState(reply.TaskId)
		}else{
			reply.TaskType = WaitTask
			alldone := c.TaskMeta.checkTaskDone()
			if alldone{
				c.ToNextState()
			}
		}
	}
	case ReducingPhase:{
		if len(c.ReduceTaskChan)>0{
			*reply = *<-c.ReduceTaskChan
			c.TaskMeta.ChangeState(reply.TaskId)
		}else{
			reply.TaskType = WaitTask
			alldone := c.TaskMeta.checkTaskDone()
			if alldone{
				c.ToNextState()
			}
		}
	}
	case AllDone:{
		reply.TaskType = ExitTask
	}
	}
	return nil
}
func (t *TaskMeta) ChangeState(taskId int) bool {
	taskInfo, ok := t.MetaMap[taskId]
	if !ok || taskInfo.TaskState != Waitting {
		return false
	}
	taskInfo.TaskState = Working
	taskInfo.StartTime = time.Now()
	return true
}
func (c *Coordinator) SetTaskDone(args *Task, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()
	
	meta,ok := c.TaskMeta.MetaMap[args.TaskId]
	// LogPrint("Task%d %v\n",args.TaskId,meta)
	if ok && (meta.TaskState == Working){
		meta.TaskState = Done
	}
	return nil
}
//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false
	mu.Lock()
	defer mu.Unlock()
	if c.WorkingPhase==AllDone{
		ret =true
	}
	// Your code here.
	return ret
}

