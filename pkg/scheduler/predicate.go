package scheduler

import (
	log "github.com/astaxie/beego/logs"
	"github.com/bnulwh/gpushare-scheduler-extender/pkg/cache"
	apivi "k8s.io/api/core/v1"
	schedulerapi "k8s.io/kubernetes/plugin/pkg/scheduler/api/v1"
)

type Predicate struct {
	Name  string
	Func  func(pod *apivi.Pod, nodeName string, c *cache.SchedulerCache) (bool, error)
	cache *cache.SchedulerCache
}

func (p Predicate) Handler(args schedulerapi.ExtenderArgs) *schedulerapi.ExtenderFilterResult {
	pod := args.Pod
	nodeNames := *args.NodeNames
	canSchedule := make([]string, 0, len(nodeNames))
	canNotSchedule := make(map[string]string)
	log.Info("==== Begin handle scheduler extender request ====")
	log.Info("==== Pod %s in ns %s with status %s", pod.Name, pod.Namespace, pod.Status.Phase)
	log.Info("==== Node names: %s", nodeNames)

	for _, nodeName := range nodeNames {
		result, err := p.Func(&pod, nodeName, p.cache)
		if err != nil {
			log.Warning("--- Can't schedule pod %s in ns %s with node %s failed: %s",
				pod.Name, pod.Namespace, nodeName, err)
			canNotSchedule[nodeName] = err.Error()
		} else {
			if result {
				log.Info("--- Can schedule pod %s in ns %s with node %s",
					pod.Name, pod.Namespace, nodeName)
				canSchedule = append(canSchedule, nodeName)
			} else {
				log.Info("--- Can't schedule pod %s in ns %s with node %s, Predicate func failed",
					pod.Name, pod.Namespace, nodeName)

			}
		}
	}

	result := schedulerapi.ExtenderFilterResult{
		NodeNames:   &canSchedule,
		FailedNodes: canNotSchedule,
		Error:       "",
	}

	log.Info("==== Finish handle scheduler extender request ====")
	return &result
}
