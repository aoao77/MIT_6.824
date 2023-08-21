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

const (
	task_type_undefined = iota
	task_type_map
	task_type_reduce
)

// const (
// 	task_state_idle = iota
// 	task_state_processing
// 	task_state_completed
// 	task_state_failed
// )

const (
	file_state_created = iota
	file_state_allocated
	file_state_finished
)

type Task struct {
	Task_no   int
	Task_type int
	// Task_state   int
	File_name_in string
	Job_nMap     int
	Job_nReduce  int
	Condition    bool
}

type Coordinator struct {
	// Your definitions here.
	nMap               int
	nReduce            int
	nMap_finished      int
	nReduce_finished   int
	map_timer          []int64
	reduce_timer       []int64
	files_array        []string
	files_map_array    []uint8
	files_reduce_array []uint8
	mutex              sync.Mutex
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
		c.mutex.Lock()
		if c.files_map_array[i] == file_state_allocated && timeNow-c.map_timer[i] > 10 {
			c.files_map_array[i] = file_state_created
			c.map_timer[i] = 0
			// fmt.Printf("map %d failed timeout\n", i)
		}
		c.mutex.Unlock()
	}
	for i := 0; i < c.nReduce; i++ {
		c.mutex.Lock()
		if c.files_reduce_array[i] == file_state_allocated && timeNow-c.reduce_timer[i] > 10 {
			c.files_reduce_array[i] = file_state_created
			c.reduce_timer[i] = 0
			// fmt.Printf("reduce %d failed timeout\n", i)
		}
		c.mutex.Unlock()
	}
	//fork task
	if args.Task_type == task_type_undefined {
		c.mutex.Lock()
		for i, file_state := range c.files_map_array {
			if file_state == file_state_created {
				c.files_map_array[i] = file_state_allocated
				c.map_timer[i] = time.Now().Unix()

				reply.Task_no = i
				reply.Task_type = task_type_map
				reply.File_name_in = c.files_array[i]
				reply.Job_nMap = c.nMap
				reply.Job_nReduce = c.nReduce
				reply.Condition = true
				// fmt.Printf("map %d created\n", i)
				break
			}
		}
		c.mutex.Unlock()

		c.mutex.Lock()
		if c.nMap == c.nMap_finished {
			for i, file_state := range c.files_reduce_array {
				if file_state == file_state_created {
					c.files_reduce_array[i] = file_state_allocated
					c.reduce_timer[i] = time.Now().Unix()

					reply.Task_no = i
					reply.Task_type = task_type_reduce
					reply.File_name_in = ""
					reply.Job_nMap = c.nMap
					reply.Job_nReduce = c.nReduce
					reply.Condition = true
					// fmt.Printf("reduce %d created\n", i)
					break
				}
			}
		}
		c.mutex.Unlock()
	}
	return nil
}

func (c *Coordinator) Ordinator_end(args *Task, reply *Task) error {
	if args.Task_type == task_type_map {
		c.mutex.Lock()
		c.files_map_array[args.Task_no] = file_state_finished
		c.nMap_finished++
		// fmt.Printf("map %d finished\n", args.Task_no)
		c.mutex.Unlock()
	} else if args.Task_type == task_type_reduce {
		c.mutex.Lock()
		c.files_reduce_array[args.Task_no] = file_state_finished
		c.nReduce_finished++
		// fmt.Printf("reduce %d finished\n", args.Task_no)
		c.mutex.Unlock()
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
	ret := false

	//reduce
	c.mutex.Lock()
	if c.nReduce_finished == c.nReduce {
		ret = true
	}
	c.mutex.Unlock()

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
	c.nMap_finished = 0
	c.nReduce_finished = 0
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
