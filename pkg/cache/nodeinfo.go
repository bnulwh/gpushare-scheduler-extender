package cache

import (
	"fmt"
	log "github.com/astaxie/beego/logs"
	"sync"

	"github.com/bnulwh/gpushare-scheduler-extender/pkg/utils"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	OptimisticLockErrorMsg = "the object has been modified; please apply your changes to the latest version and try again"
)

// NodeInfo is node level aggregated information.
type NodeInfo struct {
	name           string
	node           *v1.Node
	devs           map[int]*DeviceInfo
	gpuCount       int
	gpuTotalMemory int
	rwmu           *sync.RWMutex
}

// Create Node Level
func NewNodeInfo(node *v1.Node) *NodeInfo {
	log.Info("NewNodeInfo() creates nodeInfo for %s", node.Name)

	devMap := map[int]*DeviceInfo{}
	for i := 0; i < utils.GetGPUCountInNode(node); i++ {
		devMap[i] = newDeviceInfo(i, uint(utils.GetTotalGPUMemory(node)/utils.GetGPUCountInNode(node)))
	}

	return &NodeInfo{
		name:           node.Name,
		node:           node,
		devs:           devMap,
		gpuCount:       utils.GetGPUCountInNode(node),
		gpuTotalMemory: utils.GetTotalGPUMemory(node),
		rwmu:           new(sync.RWMutex),
	}
}

func (n *NodeInfo) GetName() string {
	return n.name
}

func (n *NodeInfo) GetDevs() []*DeviceInfo {
	devs := make([]*DeviceInfo, n.gpuCount)
	for i, dev := range n.devs {
		devs[i] = dev
	}
	return devs
}

func (n *NodeInfo) GetNode() *v1.Node {
	return n.node
}

func (n *NodeInfo) GetTotalGPUMemory() int {
	return n.gpuTotalMemory
}

func (n *NodeInfo) GetGPUCount() int {
	return n.gpuCount
}

func (n *NodeInfo) removePod(pod *v1.Pod) {
	n.rwmu.Lock()
	defer n.rwmu.Unlock()

	id := utils.GetGPUIDFromAnnotation(pod)
	log.Info("POD %s in ns %s,it's GPU id is %d", pod.Name, pod.Namespace, id)
	if id >= 0 {
		dev, found := n.devs[id]
		if !found {
			log.Warning("warn: Pod %s in ns %s failed to find the GPU ID %d in node %s", pod.Name, pod.Namespace, id, n.name)
		} else {
			log.Info("POD %s in ns %s,it's GPU dev is %d", pod.Name, pod.Namespace, dev.idx)
			dev.removePod(pod)
		}
	} else {
		log.Warning("warn: Pod %s in ns %s is not set the GPU ID %d in node %s", pod.Name, pod.Namespace, id, n.name)
	}
}

// Add the Pod which has the GPU id to the node
func (n *NodeInfo) addOrUpdatePod(pod *v1.Pod) (added bool) {
	n.rwmu.Lock()
	defer n.rwmu.Unlock()

	id := utils.GetGPUIDFromAnnotation(pod)
	log.Debug("addOrUpdatePod() Pod %s in ns %s with the GPU ID %d should be added to device map",
		pod.Name, pod.Namespace, id)
	if id >= 0 {
		dev, found := n.devs[id]
		if !found {
			log.Warning("Pod %s in ns %s failed to find the GPU ID %d in node %s", pod.Name, pod.Namespace, id, n.name)
		} else {
			dev.addPod(pod)
			added = true
		}
	} else {
		log.Warning("Pod %s in ns %s is not set the GPU ID %d in node %s", pod.Name, pod.Namespace, id, n.name)
	}
	return added
}

// check if the pod can be allocated on the node
func (n *NodeInfo) Assume(pod *v1.Pod) (allocatable bool) {
	allocatable = false
	log.Info("Begin Assume pos %s in ns %s", pod.Name, pod.Namespace)
	defer log.Info("Finish Assume pos %s in ns %s,allocatable: %v", pod.Name, pod.Namespace, allocatable)
	n.rwmu.RLock()
	defer n.rwmu.RUnlock()

	availableGPUs := n.getAvailableGPUs()
	reqGPU := uint(utils.GetGPUMemoryFromPodResource(pod))
	log.Info("AvailableGPUs: %v in node %s", availableGPUs, n.name)
	log.Info("Pod %s in ns %s need %s %s Mib", pod.Name, pod.Namespace, utils.ResourceName, reqGPU)
	if len(availableGPUs) > 0 {
		for devID := 0; devID < len(n.devs); devID++ {
			availableGPU, ok := availableGPUs[devID]
			if ok {
				if availableGPU >= reqGPU {
					allocatable = true
					break
				}
			}
		}
	}

	return allocatable

}

func (n *NodeInfo) Allocate(clientset *kubernetes.Clientset, pod *v1.Pod) (err error) {
	log.Info("Allocate() ----Begin to allocate GPU for gpu mem for pod %s in ns %s----", pod.Name, pod.Namespace)
	defer log.Info("Allocate() ----End to allocate GPU for gpu mem for pod %s in ns %s----", pod.Name, pod.Namespace)

	var newPod *v1.Pod
	n.rwmu.Lock()
	defer n.rwmu.Unlock()
	// 1. Update the pod spec
	devId, found := n.allocateGPUID(pod)
	if found {
		log.Info("Allocate() 1. Allocate GPU ID %d to pod %s in ns %s.----", devId, pod.Name, pod.Namespace)
		// newPod := utils.GetUpdatedPodEnvSpec(pod, devId, nodeInfo.GetTotalGPUMemory()/nodeInfo.GetGPUCount())
		newPod = utils.GetUpdatedPodAnnotationSpec(pod, devId, n.GetTotalGPUMemory()/n.GetGPUCount())
		_, err = clientset.CoreV1().Pods(newPod.Namespace).Update(newPod)
		if err != nil {
			log.Error("Update Pod %s in ns %s with api-server failed: %s", newPod.Name, newPod.Namespace, err)
			// the object has been modified; please apply your changes to the latest version and try again
			if err.Error() == OptimisticLockErrorMsg {
				// retry
				pod, err = clientset.CoreV1().Pods(pod.Namespace).Get(pod.Name, metav1.GetOptions{})
				if err != nil {
					log.Error("Get pod %s in ns %s with api-server failed: %s", pod.Name, pod.Namespace, err)
					return err
				}
				// newPod = utils.GetUpdatedPodEnvSpec(pod, devId, nodeInfo.GetTotalGPUMemory()/nodeInfo.GetGPUCount())
				newPod = utils.GetUpdatedPodAnnotationSpec(pod, devId, n.GetTotalGPUMemory()/n.GetGPUCount())
				_, err = clientset.CoreV1().Pods(newPod.Namespace).Update(newPod)
				if err != nil {
					log.Error("Update Pod %s in ns %s with api-server failed: %s", newPod.Name, newPod.Namespace, err)
					return err
				}
			} else {
				return err
			}
		}
	} else {
		log.Error("The node %s can't place the pod %s in ns %s", pod.Spec.NodeName, pod.Name, pod.Namespace)
		err = fmt.Errorf("The node %s can't place the pod %s in ns %s", pod.Spec.NodeName, pod.Name, pod.Namespace)
	}

	// 2. Bind the pod to the node
	if err == nil {
		binding := &v1.Binding{
			ObjectMeta: metav1.ObjectMeta{Name: pod.Name, UID: pod.UID},
			Target:     v1.ObjectReference{Kind: "Node", Name: n.name},
		}
		log.Info("Allocate() 2. Try to bind pod %s in %s namespace to node %s with %v",
			pod.Name, pod.Namespace, pod.Spec.NodeName, binding)
		err = clientset.CoreV1().Pods(pod.Namespace).Bind(binding)
		if err != nil {
			log.Warning("Failed to bind the pod %s in ns %s due to %v", pod.Name, pod.Namespace, err)
			return err
		}
	}

	// 3. update the device info if the pod is update successfully
	if err == nil {
		log.Info("Allocate() 3. Try to add pod %s in ns %s to dev %d",
			pod.Name, pod.Namespace, devId)
		dev, found := n.devs[devId]
		if !found {
			log.Warning("Pod %s in ns %s failed to find the GPU ID %d in node %s", pod.Name, pod.Namespace, devId, n.name)
		} else {
			dev.addPod(newPod)
		}
	}
	return err
}

// allocate the GPU ID to the pod
func (n *NodeInfo) allocateGPUID(pod *v1.Pod) (candidateDevID int, found bool) {
	log.Info("Begin allocateGPUID for pod %s in ns %s", pod.Name, pod.Namespace)
	defer log.Info("Finish allocateGPUID for pod %s in ns %s, devid: %d", pod.Name, pod.Namespace, candidateDevID)
	reqGPU := uint(0)
	found = false
	candidateDevID = -1
	candidateGPUMemory := uint(0)
	availableGPUs := n.getAvailableGPUs()
	log.Info("availableGPUs: %v", availableGPUs)
	reqGPU = uint(utils.GetGPUMemoryFromPodResource(pod))

	if reqGPU > uint(0) {
		log.Info("reqGPU for pod %s in ns %s: %d MiB", pod.Name, pod.Namespace, reqGPU)
		log.Info("AvailableGPUs: %v in node %s", availableGPUs, n.name)
		if len(availableGPUs) > 0 {
			for devID := 0; devID < len(n.devs); devID++ {
				availableGPU, ok := availableGPUs[devID]
				if ok {
					//find the min GPU mem large than req GPU
					if availableGPU >= reqGPU {
						if candidateDevID == -1 || candidateGPUMemory > availableGPU {
							candidateDevID = devID
							candidateGPUMemory = availableGPU
						}

						found = true
					}
				}
			}
		}

		if found {
			log.Info("Find candidate dev id %d for pod %s in ns %s successfully.",
				candidateDevID, pod.Name, pod.Namespace)
		} else {
			log.Warning("Failed to find available GPUs %d for the pod %s in the namespace %s",
				reqGPU, pod.Name, pod.Namespace)
		}
	}

	return candidateDevID, found
}

func (n *NodeInfo) getAvailableGPUs() (availableGPUs map[int]uint) {
	allGPUs := n.getAllGPUs()
	usedGPUs := n.getUsedGPUs()
	availableGPUs = map[int]uint{}
	for id, totalGPUMem := range allGPUs {
		if usedGPUMem, found := usedGPUs[id]; found {
			availableGPUs[id] = totalGPUMem - usedGPUMem
		}
	}
	return availableGPUs
}

// device index: gpu memory
func (n *NodeInfo) getUsedGPUs() (usedGPUs map[int]uint) {
	usedGPUs = map[int]uint{}
	for _, dev := range n.devs {
		usedGPUs[dev.idx] = dev.GetUsedGPUMemory()
	}
	log.Info("getUsedGPUs: %v in node %s, and devs %v", usedGPUs, n.name, n.devs)
	return usedGPUs
}

// device index: gpu memory
func (n *NodeInfo) getAllGPUs() (allGPUs map[int]uint) {
	allGPUs = map[int]uint{}
	for _, dev := range n.devs {
		allGPUs[dev.idx] = dev.totalGPUMem
	}
	log.Info("getAllGPUs: %v in node %s, and dev %v", allGPUs, n.name, n.devs)
	return allGPUs
}
