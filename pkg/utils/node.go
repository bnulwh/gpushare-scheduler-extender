package utils

import "k8s.io/api/core/v1"

// Is the Node for GPU sharing
func IsGPUSharingNode(node *v1.Node) bool {
	return GetTotalGPUMemory(node) > 0
}

// Get the total GPU memory of the Node
func GetTotalGPUMemory(node *v1.Node) int {
	val, ok := node.Status.Capacity[ResourceName]

	if !ok {
		return 0
	}

	return int(val.Value())
}

//get the Allocatable total GPU memory of the Node
func GetAllocatableTotalGPUMemory(node *v1.Node) int {
	val, ok := node.Status.Allocatable[ResourceName]
	if !ok {
		return 0
	}
	return int(val.Value())
}

// Get the GPU count of the node
func GetGPUCountInNode(node *v1.Node) int {
	val, ok := node.Status.Capacity[CountName]

	if !ok {
		return int(0)
	}

	return int(val.Value())
}

// Get the Allocatable GPU count of the node
func GetAllocatableGPUCountInNode(node *v1.Node) int {
	val, ok := node.Status.Allocatable[CountName]

	if !ok {
		return int(0)
	}

	return int(val.Value())
}
