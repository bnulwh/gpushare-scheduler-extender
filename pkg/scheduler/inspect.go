package scheduler

import (
	"github.com/astaxie/beego/logs"
	log "github.com/astaxie/beego/logs"
	"github.com/bnulwh/gpushare-scheduler-extender/pkg/cache"
	"github.com/bnulwh/gpushare-scheduler-extender/pkg/utils"
)

func (in Inspect) Handler(name string) *Result {
	nodes := []*Node{}
	errMsg := ""
	log.Info("==== Begin handle inspect request ====")
	log.Info("==== node %s", name)
	if len(name) == 0 {
		nodeInfos := in.cache.GetNodeinfos()
		for _, info := range nodeInfos {
			nodes = append(nodes, buildNode(info))
		}

	} else {
		info, err := in.cache.GetNodeInfo(name)
		if err != nil {
			errMsg = err.Error()
		}
		// nodeInfos = append(nodeInfos, node)
		nodes = append(nodes, buildNode(info))
	}
	log.Info("==== End handle inspect request ====")
	return &Result{
		Nodes: nodes,
		Error: errMsg,
	}
}

func buildNode(info *cache.NodeInfo) *Node {

	devInfos := info.GetDevs()
	devs := []*Device{}
	var usedGPU uint

	for i, devInfo := range devInfos {
		dev := &Device{
			ID:       i,
			TotalGPU: devInfo.GetTotalGPUMemory(),
			UsedGPU:  devInfo.GetUsedGPUMemory(),
		}

		podInfos := devInfo.GetPods()
		pods := []*Pod{}
		for _, podInfo := range podInfos {
			if utils.AssignedNonTerminatedPod(podInfo) {
				pod := &Pod{
					Namespace: podInfo.Namespace,
					Name:      podInfo.Name,
					UsedGPU:   utils.GetGPUMemoryFromPodResource(podInfo),
				}
				pods = append(pods, pod)
			}
		}
		dev.Pods = pods
		devs = append(devs, dev)
		usedGPU += devInfo.GetUsedGPUMemory()
	}
	logs.Info("--- node: %s, total gpu mem: %v MiB, used gpu mem: %v MiB, devices: %v",
		info.GetName(), info.GetTotalGPUMemory(), usedGPU, devs)
	return &Node{
		Name:     info.GetName(),
		TotalGPU: uint(info.GetTotalGPUMemory()),
		UsedGPU:  usedGPU,
		Devices:  devs,
	}

}
