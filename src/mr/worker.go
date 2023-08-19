package mr

import (
	"fmt"
	"hash/fnv"
	"io"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	// "io/ioutil"
	"log"
	"net/rpc"
	"os"

	"encoding/json"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// if exist, err := file_exist(path); exist == true && err == nil {
func file_exist(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return true, err
}

func files_delete(path string) error {
	files, err := filepath.Glob(path)
	if err != nil {
		log.Fatalf("cannot glob %s", path)
	}
	for _, f := range files {
		if err := os.Remove(f); err != nil {
			log.Fatalf("cannot remove %s", f)
		}
	}
	return err
}

func task_exec_mapf(work_task *Task, mapf func(string, string) []KeyValue) {
	//delete temp file
	pwd, _ := os.Getwd()
	temp_name := fmt.Sprintf("mr-%d-*-temp.*", work_task.Task_no)
	files_delete(temp_name)

	//read file
	file, err := os.Open(work_task.File_name_in)
	if err != nil {
		log.Fatalf("cannot open %v", work_task.File_name_in)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", work_task.File_name_in)
	}
	file.Close()
	//func map
	intermediate := []KeyValue{}
	kva := mapf(work_task.File_name_in, string(content))
	intermediate = append(intermediate, kva...)
	//sort
	sort.Sort(ByKey(intermediate))
	//create temp file
	ofiles_temp := make([]*os.File, work_task.Job_nReduce)
	encs_temp := make([]*json.Encoder, work_task.Job_nReduce)
	for i := 0; i < work_task.Job_nReduce; i++ {
		oname := fmt.Sprintf("mr-%d-%d-temp.*", work_task.Task_no, i)
		ofile, err := os.CreateTemp(pwd, oname)
		if err != nil {
			log.Fatalf("cannot CreateTemp %v", oname)
		}
		//defer remove
		defer os.Remove(oname)
		ofiles_temp[i] = ofile
		encs_temp[i] = json.NewEncoder(ofile)
	}

	//merge
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		sum := 0
		for k := i; k < j; k++ {
			conv_int, err := strconv.Atoi(intermediate[k].Value)
			if err != nil {
				log.Fatalf("strconv failed : %s", intermediate[k].Value)
			}
			sum += conv_int
		}
		intermediate[i].Value = strconv.Itoa(sum)
		//output
		reduce_no := ihash(intermediate[i].Key) % work_task.Job_nReduce
		err := encs_temp[reduce_no].Encode(&intermediate[i])
		if err != nil {
			log.Fatalf("Encode failed : %s , %s ", intermediate[i].Key, intermediate[i].Value)
		}

		i = j
	}

	//atomic rename file
	for i := 0; i < work_task.Job_nReduce; i++ {
		oname := fmt.Sprintf("mr-%d-%d", work_task.Task_no, i)
		if exist, _ := file_exist(oname); !exist {
			err := os.Rename(ofiles_temp[i].Name(), oname)
			if err != nil {
				log.Fatalf("cannot Rename %v to %v", ofiles_temp[i].Name(), oname)
			}
		}
	}
}

func task_exec_reducef(work_task *Task, reducef func(string, []string) string) {
	//delete temp_file
	pwd, _ := os.Getwd()
	temp_name := fmt.Sprintf("mr-out-%d-temp.*", work_task.Task_no)
	files_delete(temp_name)

	//create temp_file
	oname_temp := fmt.Sprintf("mr-out-%d-temp.*", work_task.Task_no)
	ofile_temp, err := os.CreateTemp(pwd, oname_temp)
	if err != nil {
		log.Fatalf("cannot CreateTemp %v", oname_temp)
	}
	//defer remove
	defer os.Remove(oname_temp)

	intermediate := []KeyValue{}
	//open intermediate file
	for i := 0; i < work_task.Job_nMap; i++ {
		file_name_in := fmt.Sprintf("mr-%d-%d", i, work_task.Task_no)
		file, err := os.Open(file_name_in)
		if err != nil {
			log.Fatalf("cannot open %v", work_task.File_name_in)
		}

		//read intermediate file
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		//close
		file.Close()
	}
	//sort
	sort.Sort(ByKey(intermediate))

	//split
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		sum := 0
		for k := i; k < j; k++ {
			conv_int, err := strconv.Atoi(intermediate[k].Value)
			if err != nil {
				log.Fatalf("strconv failed : %s", intermediate[k].Value)
			}
			sum += conv_int
		}
		values_one := []string{}
		for k := 0; k < sum; k++ {
			values_one = append(values_one, "1")
		}

		output := reducef(intermediate[i].Key, values_one)

		fmt.Fprintf(ofile_temp, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	//atomic rename file
	oname := fmt.Sprintf("mr-out-%d", work_task.Task_no)
	if exist, _ := file_exist(oname); !exist {
		err := os.Rename(ofile_temp.Name(), oname)
		if err != nil {
			log.Fatalf("cannot Rename %v to %v", ofile_temp.Name(), oname)
		}
	}
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// defination
	work_task := Task{0, task_type_undefined, "", 0, 0, false}

	for work_task.Task_type == task_type_undefined {
		if ret := CallTaskBegin(&work_task); !ret {
			log.Fatalf("CallTaskBegin failed")
		}
		time.Sleep(time.Second)
	}

	if work_task.Task_type == task_type_map {
		task_exec_mapf(&work_task, mapf)
	} else if work_task.Task_type == task_type_reduce {
		for !work_task.Condition {
			if ret := CallTaskBegin(&work_task); !ret {
				log.Fatalf("CallTaskBegin failed")
			}
			time.Sleep(time.Second)
		}
		task_exec_reducef(&work_task, reducef)
	}

	work_task.Condition = false
	for !work_task.Condition {
		if ret := CallTaskEnd(&work_task); !ret {
			log.Fatalf("CallTaskEnd failed")
		}
		time.Sleep(time.Second)
	}
}

func CallTaskBegin(work_task *Task) bool {
	ok := call("Coordinator.Ordinator_begin", &work_task, &work_task)
	if ok {
		fmt.Printf("call TaskBegin success!\n")
	} else {
		fmt.Printf("call TaskBegin failed!\n")
	}
	return ok
}

func CallTaskEnd(work_task *Task) bool {
	ok := call("Coordinator.Ordinator_end", &work_task, &work_task)
	if ok {
		fmt.Printf("call TaskEnd success!\n")
	} else {
		fmt.Printf("call TaskEnd failed!\n")
	}
	return ok
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
