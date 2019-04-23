package utils

import (
	"encoding/json"
	"fmt"
	log "github.com/astaxie/beego/logs"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"k8s.io/client-go/kubernetes"

	"k8s.io/api/core/v1"
)

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

// GetGPUMemoryFromPodAnnotation gets the GPU Memory of the node
func GetGPUMemoryFromNodeStatus(node *v1.Node) (gpuMemory uint) {
	quantity, found := node.Status.Capacity[ResourceName]
	if found {
		s, ok := quantity.AsInt64()
		if ok {
			gpuMemory += uint(s)
		}
	}
	log.Debug("node %s in ns %s with status %v has GPU Mem %d MiB",
		node.Name, node.Namespace, node.Status.Phase, gpuMemory)
	return gpuMemory
}

func UpdateNodeStatus(clientset *kubernetes.Clientset, oldNode *v1.Node, mem uint) (*v1.Node, error) {
	newNode := oldNode.DeepCopy()
	quantity, err := resource.ParseQuantity(fmt.Sprintf("%d", mem))
	if err != nil {
		log.Error("ParseQuantity from %d failed:  %v", mem, err)
		return oldNode, err
	}
	newNode.Status.Allocatable[ResourceName] = quantity
	nodeName := oldNode.Name

	oldData, err := json.Marshal(oldNode)
	if err != nil {
		log.Error("failed to marshal old node %#v for node %q: %v", oldNode, nodeName, err)
		return nil, fmt.Errorf("failed to marshal old node %#v for node %q: %v", oldNode, nodeName, err)
	}

	// Reset spec to make sure only patch for Status or ObjectMeta is generated.
	// Note that we don't reset ObjectMeta here, because:
	// 1. This aligns with Nodes().UpdateStatus().
	// 2. Some component does use this to update node annotations.
	newNode.Spec = oldNode.Spec
	newData, err := json.Marshal(newNode)
	if err != nil {
		log.Error("failed to marshal new node %#v for node %q: %v", newNode, nodeName, err)
		return nil, fmt.Errorf("failed to marshal new node %#v for node %q: %v", newNode, nodeName, err)
	}

	patchBytes, err := strategicpatch.CreateTwoWayMergePatch(oldData, newData, v1.Node{})
	if err != nil {
		log.Error("failed to create patch for node %q: %v", nodeName, err)
		return nil, fmt.Errorf("failed to create patch for node %q: %v", nodeName, err)
	}

	updatedNode, err := clientset.CoreV1().Nodes().Patch(string(nodeName), types.StrategicMergePatchType, patchBytes, "status")
	if err != nil {
		log.Error("failed to patch status %q for node %q: %v", patchBytes, nodeName, err)
		return nil, fmt.Errorf("failed to patch status %q for node %q: %v", patchBytes, nodeName, err)
	}
	return updatedNode, nil
}
