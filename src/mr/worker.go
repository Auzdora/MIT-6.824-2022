package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

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
	wkno := CallRegister()
	if wkno == -1 {
		return
	}

	for {
		fl, task, taskno, err := CallDeployTask(wkno)
		if err != nil {
			fmt.Println("Error:", err)
			return
		}
		if fl[0] == "WAIT" {
			// fmt.Println("wait")
			err := CallToWait(wkno)
			if err != nil {
				fmt.Println("Error:", err)
				return
			}
		}

		if fl[0] == "EXIT" {
			// fmt.Println("exit")
			return
		}

		// fmt.Println(task, fl)
		if task == MAP {
			ExecuteMap(mapf, wkno, taskno, fl)
			err := CallJobDone(wkno, task, taskno)
			if err != nil {
				fmt.Println("Error:", err)
				return
			}
		} else if task == REDUCE {
			ExcuteReduce(reducef, wkno, taskno, fl)
			err := CallJobDone(wkno, task, taskno)
			if err != nil {
				fmt.Println("Error:", err)
				return
			}
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func ExecuteMap(mapf func(string, string) []KeyValue, wkno int, taskno int, fl []string) {
	intermediate := []KeyValue{}
	for _, filename := range fl {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
	}
	emitIntermediate(taskno, intermediate)
}

func ExcuteReduce(reducef func(string, []string) string, wkno int, taskno int, fl []string) {
	kva := []KeyValue{}
	for _, filename := range fl {
		file, err := os.OpenFile(filename, os.O_RDWR, os.ModeAppend)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
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
	sort.Sort(ByKey(kva))
	// fmt.Println(kva)
	oname := "mr-out-" + strconv.Itoa(taskno)
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	ofile.Close()
}

// emit the intermediate KV pairs to the certain files
func emitIntermediate(taskno int, intm []KeyValue) {
	filelist := make([]*os.File, 0)
	fnlist := make([]string, 0)
	enclist := make([]*json.Encoder, 0)
	for i := 0; i < 10; i++ {
		fn := "mr-" + strconv.Itoa(taskno) + "-" + strconv.Itoa(i)
		fnlist = append(fnlist, fn)
		file := lookupFiles("./" + fn)
		filelist = append(filelist, file)
		enc := json.NewEncoder(filelist[i])
		enclist = append(enclist, enc)
	}

	for _, kv := range intm {
		fn := ihash(kv.Key) % 10
		err := enclist[fn].Encode(&kv)
		if err != nil {
			fmt.Println("Error:", err)
			return
		}
	}

	err := CallRegisterInterFile(fnlist)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

}

// if a file exsit, if exsit, return files, ortherwise create and return
func lookupFiles(path string) *os.File {
	_, err := os.Stat(path)
	if err == nil {
		file, err := os.OpenFile(path, os.O_RDWR, os.ModeAppend)
		if err != nil {
			fmt.Println("Error:", err)
			return nil
		}
		return file
	}
	if os.IsNotExist(err) {
		file, err := os.Create(path)
		if err != nil {
			fmt.Println("Error:", err)
			return nil
		}
		return file
	}
	return nil
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

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

// call RPC register function to register current worker
func CallRegister() int {
	args := RegisterArgs{}
	reply := RegisterReply{}

	ok := call("Coordinator.Register", &args, &reply)
	if ok {
		return reply.Wkno
	}
	fmt.Println("call failed!")
	return -1
}

// ask for a map task to the coordinator
func CallDeployTask(wkno int) ([]string, int, int, error) {

	// declare an argument
	args := TaskArgs{}
	args.Wkno = wkno

	// declare a task reply structure
	reply := TaskReply{}

	ok := call("Coordinator.DeployTask", &args, &reply)
	if ok {
		return reply.Filelist, reply.Task, reply.Taksno, nil
	}
	var e error = errors.New("call failed error")
	return nil, -1, -1, e
}

// let a worker wait
func CallToWait(wkno int) error {
	args := WaitArgs{Wkno: wkno}
	reply := WaitReply{}

	ok := call("Coordinator.ToWait", &args, &reply)
	if ok {
		return nil
	}
	var e error = errors.New("call failed error")
	return e
}

// tell coordinator the job is done
func CallJobDone(wkno int, task int, taskno int) error {
	args := JobDoneArgs{Wkno: wkno, Task: task, Taskno: taskno}
	reply := JobDoneReply{}

	ok := call("Coordinator.JobDone", &args, &reply)
	if ok {
		return nil
	}
	var e error = errors.New("call failed error")
	return e
}

func CallRegisterInterFile(fl []string) error {
	args := FileArgs{Filelist: fl}
	reply := FileReply{}

	ok := call("Coordinator.RegisterInterFile", &args, &reply)
	if ok {
		return nil
	}
	var e error = errors.New("call failed error")
	return e
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
