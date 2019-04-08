package cache

import (
	log "github.com/astaxie/beego/logs"
	"sync"

	"github.com/bnulwh/gpushare-scheduler-extender/pkg/utils"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	corelisters "k8s.io/client-go/listers/core/v1"
)

type SchedulerCache struct {
	Namespace string
	// a map from pod key to podState.
	nodes map[string]*NodeInfo

	// nodeLister can list/get nodes from the shared informer's store.
	nodeLister corelisters.NodeLister

	//
	podLister corelisters.PodLister

	// record the knownPod, it will be added when annotation SHARED_GPU_ID is added, and will be removed when complete and deleted
	knownPods map[types.UID]*v1.Pod
	nLock     *sync.RWMutex
}

func NewSchedulerCache(namespace string, nLister corelisters.NodeLister, pLister corelisters.PodLister) *SchedulerCache {
	log.Info("Create scheduler cache ok")
	return &SchedulerCache{
		Namespace:  namespace,
		nodes:      make(map[string]*NodeInfo),
		nodeLister: nLister,
		podLister:  pLister,
		knownPods:  make(map[types.UID]*v1.Pod),
		nLock:      new(sync.RWMutex),
	}
}

func (cache *SchedulerCache) GetNodeinfos() []*NodeInfo {
	nodes := []*NodeInfo{}
	for _, n := range cache.nodes {
		nodes = append(nodes, n)
	}
	return nodes
}

// build cache when initializing
func (cache *SchedulerCache) BuildCache() error {
	log.Info("begin to build scheduler cache")
	defer log.Info("end to build scheduler cache")
	err := cache.buildNodeCache()
	err = cache.buildPodCache()
	return err
}

func (cache *SchedulerCache) GetPod(name, namespace string) (*v1.Pod, error) {
	return cache.podLister.Pods(namespace).Get(name)
}

// Get known pod from the pod UID
func (cache *SchedulerCache) KnownPod(podUID types.UID) bool {
	cache.nLock.RLock()
	defer cache.nLock.RUnlock()

	_, found := cache.knownPods[podUID]
	return found
}

func (cache *SchedulerCache) AddOrUpdatePod(pod *v1.Pod) error {
	log.Info("Begin Add or update pod %s in ns %s from cache", pod.Name, pod.Namespace)
	defer log.Info("Finish Add or update pod %s in ns %s from cache", pod.Name, pod.Namespace)
	log.Info("Pod info: %v", pod)
	log.Info("Nodes %v", cache.nodes)
	if len(pod.Spec.NodeName) == 0 {
		log.Warning("pod %s in ns %s is not assigned to any node, skip", pod.Name, pod.Namespace)
		return nil
	}

	n, err := cache.GetNodeInfo(pod.Spec.NodeName)
	if err != nil {
		log.Warning("get node %s from cache failed: %s", pod.Spec.NodeName, err)
		return err
	}
	podCopy := pod.DeepCopy()
	if n.addOrUpdatePod(podCopy) {
		// put it into known pod
		cache.rememberPod(pod.UID, podCopy)
	} else {
		log.Warning("pod %s in ns %s's gpu id is %d, it's illegal, skip",
			pod.Name, pod.Namespace, utils.GetGPUIDFromAnnotation(pod))
	}

	return nil
}
func (cache *SchedulerCache) AddOrUpdateNode(node *v1.Node) error {
	log.Info("Begin Add or update node %s in ns %s from cache", node.Name, node.Namespace)
	defer log.Info("Finish Add or update node %s in ns %s from cache", node.Name, node.Namespace)
	log.Info("Node info: %v", node)
	_, err := cache.GetNodeInfo(node.Name)
	return err
}

// The lock is in cacheNode
func (cache *SchedulerCache) RemovePod(pod *v1.Pod) {
	log.Info("Begin remove pod %s in ns %s from cache", pod.Name, pod.Namespace)
	defer log.Info("Finish remove pod %s in ns %s from cache", pod.Name, pod.Namespace)
	log.Debug("Remove pod info: %v", pod)
	log.Debug("Node %v", cache.nodes)
	n, err := cache.GetNodeInfo(pod.Spec.NodeName)
	if err == nil {
		n.removePod(pod)
	} else {
		log.Warning("Failed to get node %s due to %v", pod.Spec.NodeName, err)
	}
	cache.forgetPod(pod.UID)
}

// Get or build nodeInfo if it doesn't exist
func (cache *SchedulerCache) GetNodeInfo(name string) (*NodeInfo, error) {
	log.Info("Begin get node %s from cache", name)
	defer log.Info("Finish get node %s from cache", name)
	node, err := cache.nodeLister.Get(name)
	if err != nil {
		log.Warning("get node %s from cache.nodeLister failed: %s", name, err)
		return nil, err
	}

	cache.nLock.Lock()
	defer cache.nLock.Unlock()
	n, ok := cache.nodes[name]

	if !ok {
		n = NewNodeInfo(node)
		cache.nodes[name] = n
	} else {
		// if the existing node turn from non gpushare to gpushare
		if (utils.GetTotalGPUMemory(n.node) <= 0 && utils.GetTotalGPUMemory(node) > 0) ||
			(utils.GetGPUCountInNode(n.node) <= 0 && utils.GetGPUCountInNode(node) > 0) {
			node, err := cache.nodeLister.Get(name)
			if err != nil {
				log.Warning("get node %s from cache.nodeLister failed: %s", name, err)
				return nil, err
			}
			log.Info("GetNodeInfo() need update node %s from %v to %v",
				name, n.node, node)
			n = NewNodeInfo(node)
			cache.nodes[name] = n
		}

		log.Info("GetNodeInfo() uses the existing nodeInfo for %s", name)
	}
	return n, nil
}

func (cache *SchedulerCache) forgetPod(uid types.UID) {
	cache.nLock.Lock()
	defer cache.nLock.Unlock()
	delete(cache.knownPods, uid)
}

func (cache *SchedulerCache) rememberPod(uid types.UID, pod *v1.Pod) {
	cache.nLock.Lock()
	defer cache.nLock.Unlock()
	cache.knownPods[pod.UID] = pod
}

func createSelector() labels.Selector {
	selector := labels.NewSelector()
	var vals []string
	requirement, err := labels.NewRequirement(utils.ResourceName, "exists", vals)
	if err != nil {
		log.Warning("create requirment failed: %s", err)
	}
	selector.Add(*requirement)
	return selector
}

func (cache *SchedulerCache) buildPodCache() error {
	selector := createSelector()
	pods, err := cache.podLister.Pods(cache.Namespace).List(selector)
	if err != nil {
		log.Warning("list pods failed: %s", err)
		return err
	} else {
		for _, pod := range pods {
			log.Info("begin build cache from pod %s in ns %s", pod.Name, pod.Namespace)
			if utils.GetGPUMemoryFromPodAnnotation(pod) <= uint(0) {
				continue
			}

			if len(pod.Spec.NodeName) == 0 {
				continue
			}

			err = cache.AddOrUpdatePod(pod)
			if err != nil {
				log.Warning("cache add or update pod %s in ns %s failed: %s", pod.Name, pod.Namespace, err)
				return err
			}
		}

	}

	return nil

}
func (cache *SchedulerCache) buildNodeCache() error {
	nodes, err := cache.nodeLister.List(labels.Everything())
	if err != nil {
		log.Warning("list nodes failed: %s", err)
		return err
	} else {
		for _, node := range nodes {
			log.Info("begin build cache from node %s in ns %s", node.Name, node.Namespace)
			if utils.GetGPUMemoryFromNodeStatus(node) <= uint(0) {
				continue
			}

			err = cache.AddOrUpdateNode(node)
			if err != nil {
				log.Warning("cache add or update node %s in ns %s failed: %s", node.Name, node.Namespace, err)
				return err
			} else {
				log.Info("==== add node %s to cache", node.Name)
			}
		}

	}

	return nil

}
