package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

const (
	task_type_undefined = iota
	task_type_map
	task_type_reduce
)

// const (
// 	work_state_idle = iota
// 	work_state_processing
// 	work_state_completed
// 	work_state_failed
// )

const (
	file_state_created = iota
	file_state_allocated
	file_state_finished
)

type Task struct {
	Task_no      int
	Task_type    int
	File_name_in string
	Job_nMap     int
	Job_nReduce  int
	Condition    bool
}

type Coordinator struct {
	// Your definitions here.
	nMap               int
	nReduce            int
	map_timer          []int64
	reduce_timer       []int64
	files_array        []string
	files_map_array    []uint8
	files_reduce_array []uint8
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) Ordinator_begin(args *Task, reply *Task) error {
	//Time out
	timeNow := time.Now().Unix()
	for i := 0; i < c.nMap; i++ {
		if c.files_map_array[i] == file_state_allocated && timeNow-c.map_timer[i] > 10 {
			c.files_map_array[i] = file_state_created
			c.map_timer[i] = 0
		}
	}
	for i := 0; i < c.nReduce; i++ {
		if c.files_reduce_array[i] == file_state_allocated && timeNow-c.reduce_timer[i] > 10 {
			c.files_reduce_array[i] = file_state_created
			c.reduce_timer[i] = 0
		}
	}
	//fork task
	if args.Task_type == task_type_undefined {
		task_allocated := false
		for i, file_state := range c.files_map_array {
			if file_state == file_state_created {
				reply.Task_no = i
				reply.Task_type = task_type_map
				reply.File_name_in = c.files_array[i]
				reply.Job_nMap = c.nMap
				reply.Job_nReduce = c.nReduce
				reply.Condition = true

				c.files_map_array[i] = file_state_allocated
				c.map_timer[i] = time.Now().Unix()
				task_allocated = true
				break
			}
		}

		if !task_allocated {
			for i, file_state := range c.files_reduce_array {
				if file_state == file_state_created {
					reply.Task_no = i
					reply.Task_type = task_type_reduce
					reply.File_name_in = ""
					reply.Job_nMap = c.nMap
					reply.Job_nReduce = c.nReduce
					reply.Condition = false

					condition_flag := true
					for _, val := range c.files_map_array {
						if val != file_state_finished {
							condition_flag = false
							break
						}
					}
					if condition_flag {
						reply.Condition = true
						c.files_reduce_array[i] = file_state_allocated
						c.reduce_timer[i] = time.Now().Unix()
					}
					break
				}
			}
		}
	} else if args.Task_type == task_type_reduce {
		if c.files_reduce_array[args.Task_no] == file_state_created {
			condition_flag := true
			for _, val := range c.files_map_array {
				if val != file_state_finished {
					condition_flag = false
					break
				}
			}
			if condition_flag {
				reply.Condition = true
				c.files_reduce_array[args.Task_no] = file_state_allocated
				c.reduce_timer[args.Task_no] = time.Now().Unix()
			}
		}
	}
	return nil
}

func (c *Coordinator) Ordinator_end(args *Task, reply *Task) error {
	if args.Task_type == task_type_map {
		c.files_map_array[args.Task_no] = file_state_finished
	} else if args.Task_type == task_type_reduce {
		c.files_reduce_array[args.Task_no] = file_state_finished
	}
	reply.Condition = true

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
	ret := true

	//reduce
	for i := 0; i < len(c.files_reduce_array); i++ {
		if c.files_reduce_array[i] != file_state_finished {
			ret = false
			break
		}
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	//input argument
	c.nMap = len(files)
	c.nReduce = nReduce
	c.files_array = files
	c.files_map_array = make([]uint8, len(files))
	for i := 0; i < len(c.files_map_array); i++ {
		c.files_map_array[i] = file_state_created
	}
	c.files_reduce_array = make([]uint8, nReduce)
	for i := 0; i < len(c.files_reduce_array); i++ {
		c.files_reduce_array[i] = file_state_created
	}
	c.map_timer = make([]int64, len(files))
	for i := 0; i < len(c.map_timer); i++ {
		c.map_timer[i] = 0
	}
	c.reduce_timer = make([]int64, nReduce)
	for i := 0; i < len(c.reduce_timer); i++ {
		c.reduce_timer[i] = 0
	}
	c.server()
	return &c
}
