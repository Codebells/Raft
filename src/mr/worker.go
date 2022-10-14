package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import (
	"os"
	"io/ioutil"
	"time"
	"strconv"
	"encoding/json"
	"sort"
)
//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
type WorkKey []KeyValue

// for sorting by key.
func (a WorkKey) Len() int           { return len(a) }
func (a WorkKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a WorkKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
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

	// Your worker implementation here.
	exitFlag :=false
	for !exitFlag {
		task := PullTask()
		switch task.TaskType{
		case MapTask:
			DoMapTask(mapf,&task)
			CallDone(&task)
		case ReduceTask:
			DoReduceTask(reducef,&task)
			CallDone(&task)
		case WaitTask:
			// LogPrint("Task is underprogressing")
			DoWaitTask(&task)
		case ExitTask:
			time.Sleep(time.Second)
			LogPrint("Task %d is terminated!",task.TaskId)
			exitFlag=true
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func PullTask() Task{
	args := TaskArgs{false}
	reply := Task{}
	ok := call("Coordinator.PullTask", &args, &reply)
	if !ok {
		LogPrint("PullTask failed!")
	}
	return reply
}
func DoMapTask(mapf func(string, string) []KeyValue, response *Task){
	filename := response.Files[0]
	file,err := os.Open(filename)
	if err!=nil {
		log.Fatalf("Can't open %v\n",filename)
	}
	content,err := ioutil.ReadAll(file)
	if err!=nil {
		log.Fatalf("Can't read %v\n",filename)
	}
	file.Close()
	intermediate := mapf(filename,string(content))
	reduceNum := response.ReducerNumber
	SplitKV := make([][]KeyValue,reduceNum)
	for _,kv := range intermediate{
		SplitKV[ihash(kv.Key)%reduceNum]=append(SplitKV[ihash(kv.Key)%reduceNum],kv)
	}
	for i := 0 ; i < reduceNum; i++ {
		oname := "mr-tmp-" + strconv.Itoa(response.TaskId)+"-"+strconv.Itoa(i)
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile)
		for _, kv := range SplitKV[i] {
			err := enc.Encode(kv)
			if err != nil {
				return
			}
		}
		ofile.Close()
	}

}
func DoReduceTask(reducef func(string, []string) string, response *Task){
	reducerNumber := response.TaskId
	intermediate := processFile(response.Files)
	tem_dir,_ :=os.Getwd()
	temFile, err := ioutil.TempFile(tem_dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Can't create temp file", err)
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
		fmt.Fprintf(temFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	// LogPrint("DoReduceTask")
	temFile.Close()
	fname := fmt.Sprintf("mr-out-%d", reducerNumber)
	os.Rename(temFile.Name(), fname)
}
func processFile(files []string) WorkKey{
	var kva WorkKey
	for _,filename := range files{
		file,err := os.Open(filename)
		if err!=nil {
			log.Fatalf("Can't open %v\n",filename)
		}
		dec := json.NewDecoder(file)
  		for {
    		var kv KeyValue
    		if err := dec.Decode(&kv); err != nil {
     		 break
   			 }
		    kva = append(kva, kv)
  		}
		file.Close()
	}
	sort.Sort(WorkKey(kva))
	return kva
}
func DoWaitTask(task *Task) {
	time.Sleep(time.Second*5)
}
func CallDone(task *Task) Task{
	args := task
	reply :=Task{}
	ok := call("Coordinator.SetTaskDone", &args, &reply)
	if !ok {
		LogPrint("SetTaskDone failed!")
	}
	return reply
}

//
// example function to show how to make an RPC call to the coordinator.
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
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}
//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
