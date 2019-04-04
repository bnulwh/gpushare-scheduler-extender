package cache

import (
	log "github.com/astaxie/beego/logs"
	"sync"

	"github.com/bnulwh/gpushare-scheduler-extender/pkg/utils"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

type DeviceInfo struct {
	idx    int
	podMap map[types.UID]*v1.Pod
	// usedGPUMem  uint
	totalGPUMem uint
	rwmu        *sync.RWMutex
}

func (d *DeviceInfo) GetPods() []*v1.Pod {
	pods := []*v1.Pod{}
	for _, pod := range d.podMap {
		pods = append(pods, pod)
	}
	return pods
}

func newDeviceInfo(index int, totalGPUMem uint) *DeviceInfo {
	return &DeviceInfo{
		idx:         index,
		totalGPUMem: totalGPUMem,
		podMap:      map[types.UID]*v1.Pod{},
		rwmu:        new(sync.RWMutex),
	}
}

func (d *DeviceInfo) GetTotalGPUMemory() uint {
	return d.totalGPUMem
}

func (d *DeviceInfo) GetUsedGPUMemory() (gpuMem uint) {
	log.Debug("debug: GetUsedGPUMemory() podMap %v, and its address is %p", d.podMap, d)
	d.rwmu.RLock()
	defer d.rwmu.RUnlock()
	for _, pod := range d.podMap {
		if pod.Status.Phase == v1.PodSucceeded || pod.Status.Phase == v1.PodFailed {
			log.Debug("debug: skip the pod %s in ns %s due to its status is %s", pod.Name, pod.Namespace, pod.Status.Phase)
			continue
		}
		memUsed := utils.GetGPUMemoryFromPodAnnotation(pod)
		log.Info("POD %s in ns %s use GPU memory %d MiB", pod.Name, pod.Namespace, memUsed)
		// gpuMem += utils.GetGPUMemoryFromPodEnv(pod)
		gpuMem += memUsed
	}
	return gpuMem
}

func (d *DeviceInfo) addPod(pod *v1.Pod) {
	log.Debug("debug: dev.addPod() Pod %s in ns %s with the GPU ID %d will be added to device map",
		pod.Name,
		pod.Namespace,
		d.idx)
	d.rwmu.Lock()
	defer d.rwmu.Unlock()
	d.podMap[pod.UID] = pod
	log.Debug("debug: dev.addPod() after updated is %v, and its address is %p",
		d.podMap,
		d)
}

func (d *DeviceInfo) removePod(pod *v1.Pod) {
	log.Debug("debug: dev.removePod() Pod %s in ns %s with the GPU ID %d will be removed from device map",
		pod.Name,
		pod.Namespace,
		d.idx)
	d.rwmu.Lock()
	defer d.rwmu.Unlock()
	delete(d.podMap, pod.UID)
	log.Debug("debug: dev.removePod() after updated is %v, and its address is %p",
		d.podMap,
		d)
}
