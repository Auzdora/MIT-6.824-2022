package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type RegisterArgs struct{}

type RegisterReply struct {
	Wkno int
}

type TaskArgs struct {
	Wkno int
}
type TaskReply struct {
	Task     int
	Taksno   int
	Filelist []string
}

type WaitArgs struct {
	Wkno int
}
type WaitReply struct{}

type JobDoneArgs struct {
	Wkno   int
	Task   int
	Taskno int
}
type JobDoneReply struct{}

type FileArgs struct {
	Filelist []string
}
type FileReply struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
