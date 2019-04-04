package scheduler

import (
	"fmt"
	log "github.com/astaxie/beego/logs"

	"github.com/bnulwh/gpushare-scheduler-extender/pkg/cache"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

const (
	OptimisticLockErrorMsg = "the object has been modified; please apply your changes to the latest version and try again"
)

func NewGPUShareBind(clientset *kubernetes.Clientset, c *cache.SchedulerCache) *Bind {
	return &Bind{
		Name: "gpusharingbinding",
		Func: func(name string, namespace string, podUID types.UID, node string, c *cache.SchedulerCache) error {
			log.Info("==== Begin Bind Func ====")
			pod, err := getPod(name, namespace, podUID, clientset, c)
			if err != nil {
				log.Warning("Failed to handle pod %s in ns %s due to error %v", name, namespace, err)
				return err
			}
			log.Info("Get Pod %s in ns %s Success", name, namespace)
			nodeInfo, err := c.GetNodeInfo(node)
			if err != nil {
				log.Warning("Failed to handle pod %s in ns %s due to error %v", name, namespace, err)
				return err
			}
			log.Info("Get Node %s  Success", node)
			err = nodeInfo.Allocate(clientset, pod)
			if err != nil {
				log.Warning("Failed to handle pod %s in ns %s due to error %v", name, namespace, err)
				return err
			}
			log.Info("Allocate Pod %s in ns %s with node %s success.", name, namespace, node)
			log.Info("==== END Bind Func ====")
			return nil
		},
		cache: c,
	}
}

func getPod(name string, namespace string, podUID types.UID, clientset *kubernetes.Clientset, c *cache.SchedulerCache) (*v1.Pod, error) {
	pod, err := c.GetPod(name, namespace)
	if err != nil {
		log.Warning("--- Failed to handle pod %s in ns %s due to error %v", name, namespace, err)
		return nil, err
	}
	log.Info("Get Pod %s in ns % from cache success", name, namespace)
	if pod.UID != podUID {
		log.Info("not expected podUID %v, get pod from api-server", podUID)
		pod, err = clientset.CoreV1().Pods(namespace).Get(name, metav1.GetOptions{})
		if err != nil {
			log.Warning("Get pod %s in ns %s from api-server failed: %s", name, namespace, err)
			return nil, err
		}
		log.Info("Get Pod %s in ns % from api-server success", name, namespace)
		if pod.UID != podUID {
			log.Warning("The pod %s in ns %s's uid is %v, and it's not equal with expected %v",
				name, namespace, pod.UID, podUID)
			return nil, fmt.Errorf("The pod %s in ns %s's uid is %v, and it's not equal with expected %v",
				name, namespace, pod.UID, podUID)
		}
	}

	return pod, nil
}
