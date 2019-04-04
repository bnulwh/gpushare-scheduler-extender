package scheduler

import (
	log "github.com/astaxie/beego/logs"
	"github.com/bnulwh/gpushare-scheduler-extender/pkg/cache"
	"k8s.io/apimachinery/pkg/types"
	schedulerapi "k8s.io/kubernetes/plugin/pkg/scheduler/api/v1"
)

// Bind is responsible for binding node and pod
type Bind struct {
	Name  string
	Func  func(podName string, podNamespace string, podUID types.UID, node string, cache *cache.SchedulerCache) error
	cache *cache.SchedulerCache
}

// Handler handles the Bind request
func (b Bind) Handler(args schedulerapi.ExtenderBindingArgs) *schedulerapi.ExtenderBindingResult {
	log.Info("==== Begin handle binding ====")
	err := b.Func(args.PodName, args.PodNamespace, args.PodUID, args.Node, b.cache)
	errMsg := ""
	if err != nil {
		log.Error("Binding Pod %s in ns %s with node %s failed: %s",
			args.PodName, args.PodNamespace, args.Node, err)
		errMsg = err.Error()
	}
	log.Info("==== Begin handle binding ====")
	return &schedulerapi.ExtenderBindingResult{
		Error: errMsg,
	}
}
