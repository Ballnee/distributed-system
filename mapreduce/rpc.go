package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"time"
)
import "strconv"

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

//task state
type State int

const (
	Map State = iota
	Reduce
	Wait
	Exit
)

//represent worker state
type CoordinatorTaskStatus int

const (
	Idle CoordinatorTaskStatus = iota
	InProcess
	Completed
)

type Task struct {
	Input  string
	State  State
	TaskId int

	NReduce      int
	Intermediate []string //path of Intermediate files
	Output       string
}

type CoordinatorTask struct {
	TaskStatus    CoordinatorTaskStatus
	StartTime     time.Time
	TaskReference *Task
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
