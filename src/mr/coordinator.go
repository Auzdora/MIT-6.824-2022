package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	// task state
	UNSTARTED = 1
	WORKING   = 2
	DONE      = 3
	// worker state
	IDLE    = 4
	RUNNING = 5
	DIED    = 6
	// what task
	NONE   = 7
	MAP    = 8
	REDUCE = 9
)

type TaskInfo struct {
	taskno   int
	state    int
	filelist []string
}

type WokerInfo struct {
	wkno    int
	state   int
	task    int
	taskno  int
	runtime time.Time
}

type Coordinator struct {
	// Your definitions here.
	lock       sync.Mutex
	MapTask    []TaskInfo
	ReduceTask []TaskInfo
	Wokers     []WokerInfo
	nwoker     int
	nmap       int
	nreduce    int
	nmtaskd    int // number of map task done
	nrtaskd    int // number of reduce task done
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// worker register RPC
func (c *Coordinator) Register(args *RegisterArgs, reply *RegisterReply) error {
	worker := WokerInfo{}
	worker.state = IDLE
	worker.task = NONE

	c.lock.Lock()
	worker.wkno = c.nwoker
	reply.Wkno = c.nwoker
	c.nwoker++
	c.Wokers = append(c.Wokers, worker)
	c.lock.Unlock()

	return nil
}

func (c *Coordinator) DeployTask(args *TaskArgs, reply *TaskReply) error {
	c.lock.Lock()
	if c.Wokers[args.Wkno].state != IDLE {
		reply.Filelist = append(reply.Filelist, "ERROR")
		c.lock.Unlock()
		return nil
	}

	if c.nmtaskd < c.nmap {
		for idx := range c.MapTask {
			if c.MapTask[idx].state == UNSTARTED {
				c.workerStateChange(args.Wkno, MAP, idx, RUNNING, time.Now())
				c.MapTask[idx].state = WORKING
				reply.Task = MAP
				reply.Taksno = c.MapTask[idx].taskno
				reply.Filelist = c.MapTask[idx].filelist
				c.lock.Unlock()
				return nil
			}
		}

		c.insReply(args, reply, "WAIT")
		c.lock.Unlock()
		return nil
	}

	if c.nrtaskd < c.nreduce {
		for idx := range c.ReduceTask {
			if c.ReduceTask[idx].state == UNSTARTED {
				c.workerStateChange(args.Wkno, REDUCE, idx, RUNNING, time.Now())
				c.ReduceTask[idx].state = WORKING
				reply.Task = REDUCE
				reply.Taksno = c.ReduceTask[idx].taskno
				reply.Filelist = c.ReduceTask[idx].filelist
				c.lock.Unlock()
				return nil
			}
		}

		c.insReply(args, reply, "WAIT")
		c.lock.Unlock()
		return nil
	}

	if c.nrtaskd == c.nreduce {
		c.insReply(args, reply, "EXIT")
		c.lock.Unlock()
		return nil
	}

	reply.Filelist = append(reply.Filelist, "ERROR")
	c.lock.Unlock()
	return nil
}

// for simplify the code, call when hold the lock
func (c *Coordinator) workerStateChange(
	wkno int,
	task int,
	taskno int,
	state int,
	runtime time.Time,
) {
	c.Wokers[wkno].task = task
	c.Wokers[wkno].taskno = taskno
	c.Wokers[wkno].state = state
	c.Wokers[wkno].runtime = runtime
}

// reply and set for specific instructions
func (c *Coordinator) insReply(args *TaskArgs, reply *TaskReply, instruction string) {
	c.Wokers[args.Wkno].task = NONE
	c.Wokers[args.Wkno].state = IDLE
	reply.Task = NONE
	reply.Filelist = append(reply.Filelist, instruction)
}

// let a worker wait here
func (c *Coordinator) ToWait(args *WaitArgs, reply *WaitReply) error {
	c.lock.Lock()
	if c.nmtaskd != c.nmap {
		c.lock.Unlock()
		time.Sleep(100000000)
	} else {
		c.lock.Unlock()
	}
	return nil
}

// respond to workers job done
func (c *Coordinator) JobDone(args *JobDoneArgs, reply *JobDoneReply) error {
	if args.Task == MAP {
		c.lock.Lock()
		c.nmtaskd++
		c.MapTask[args.Taskno].state = DONE
		c.Wokers[args.Wkno].task = NONE
		c.Wokers[args.Wkno].state = IDLE
		c.lock.Unlock()
	} else if args.Task == REDUCE {
		c.lock.Lock()
		c.nrtaskd++
		c.ReduceTask[args.Taskno].state = DONE
		c.Wokers[args.Wkno].task = NONE
		c.Wokers[args.Wkno].state = IDLE
		c.lock.Unlock()
	}

	return nil
}

// register intermediate file to the reduce task table
func (c *Coordinator) RegisterInterFile(args *FileArgs, reply *FileReply) error {
	c.lock.Lock()
	for i := 0; i < c.nreduce; i++ {
		c.ReduceTask[i].filelist = append(c.ReduceTask[i].filelist, args.Filelist[i])
	}
	c.lock.Unlock()
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.lock.Lock()
	if c.nrtaskd == c.nreduce {
		ret = true
	}
	c.lock.Unlock()

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.nwoker = 0
	c.Wokers = make([]WokerInfo, 0)
	c.nreduce = nReduce

	fl := GetFile("../", ".txt")
	c.nmap = len(fl)
	for i := 0; i < c.nmap; i++ {
		mt := TaskInfo{taskno: i, state: UNSTARTED}
		mt.filelist = append(mt.filelist, "../"+fl[i])
		c.MapTask = append(c.MapTask, mt)
	}

	for i := 0; i < nReduce; i++ {
		rt := TaskInfo{taskno: i, state: UNSTARTED}
		c.ReduceTask = append(c.ReduceTask, rt)
	}
	go c.WorkTime()

	c.server()
	return &c
}

// get the specify ext, e.g., ".txt", in dirpath directory
func GetFile(dirpath string, exts string) []string {
	dirs, err := os.ReadDir(dirpath)
	if err != nil {
		fmt.Println("ReadDir Error:", err)
	}
	txtFiles := make([]string, 0)
	for _, entry := range dirs {
		if ext := filepath.Ext(entry.Name()); ext == exts && string(entry.Name()[0]) == "p" {
			txtFiles = append(txtFiles, entry.Name())
		}
	}
	return txtFiles
}

func (c *Coordinator) WorkTime() {
	for {
		c.lock.Lock()
		for idx := 0; idx < c.nwoker; idx++ {
			if cost := time.Since(c.Wokers[idx].runtime).Seconds(); c.Wokers[idx].state == RUNNING && cost > 10 {
				c.Wokers[idx].state = DIED
				taskno := c.Wokers[idx].taskno
				if c.Wokers[idx].task == MAP {
					c.MapTask[taskno].state = UNSTARTED
				} else if c.Wokers[idx].task == REDUCE {
					c.ReduceTask[taskno].state = UNSTARTED
				}
				c.Wokers[idx].task = NONE
			}
		}
		c.lock.Unlock()
		time.Sleep(1000000000)
	}
}
