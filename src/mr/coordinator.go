package mr

import (
	"fmt"
)

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) Coordinator {
	// c := MakeGRPCCoordinator(files, nReduce)
	c := MakeRPCCoordinator(files, nReduce)
	c.server()
	return c
}

func GenerateFileNames(pos int, numberOfFiles int) []string {
	fileNames := make([]string, numberOfFiles)

	for i := range fileNames {
		fileNames[i] = fmt.Sprintf("mr-%v-%v", i, pos)
	}
	return fileNames
}
