package main

import (
	"flag"
	"fmt"
	log "github.com/astaxie/beego/logs"
	"github.com/bnulwh/gpushare-scheduler-extender/pkg/cache"
	"github.com/bnulwh/gpushare-scheduler-extender/pkg/utils"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/watch"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/bnulwh/gpushare-scheduler-extender/pkg/gpushare"
	"github.com/bnulwh/gpushare-scheduler-extender/pkg/routes"
	"github.com/bnulwh/gpushare-scheduler-extender/pkg/scheduler"
	"github.com/bnulwh/gpushare-scheduler-extender/pkg/utils/signals"
	"github.com/julienschmidt/httprouter"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubeinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

const RecommendedKubeConfigPathEnv = "KUBECONFIG"
const logPath = "/var/log/device-plugin"

var (
	clientset    *kubernetes.Clientset
	resyncPeriod = 30 * time.Second
	clientConfig clientcmd.ClientConfig
)

func init() {
	beegoInit()
}
func beegoInit() {
	log.EnableFuncCallDepth(true)
	log.SetLogFuncCallDepth(3)
	if !pathExists(logPath) {
		fmt.Printf("dir: %s not found.", logPath)
		err := os.MkdirAll(logPath, 0711)
		if err != nil {
			fmt.Printf("mkdir %s failed: %v", logPath, err)
		}
	}
	err := log.SetLogger(log.AdapterMultiFile, `{"filename":"/var/log/device-plugin/nvidia.log","separate":["emergency", "alert", 
			"critical", "error", "warning", "notice", "info", "debug"]}`)
	if err != nil {
		fmt.Println(err)
	}
	err = log.SetLogger(log.AdapterConsole, `{"level":6}`)
	if err != nil {
		fmt.Println(err)
	}
}

func pathExists(path string) bool {
	_, err := os.Stat(path)
	if err == nil {
		return true
	}
	if os.IsNotExist(err) {
		return false
	}
	return false
}

func initKubeClient() {
	kubeConfig := ""
	if len(os.Getenv(RecommendedKubeConfigPathEnv)) > 0 {
		// use the current context in kubeconfig
		// This is very useful for running locally.
		kubeConfig = os.Getenv(RecommendedKubeConfigPathEnv)
	}
	log.Debug("kube config path: %s", kubeConfig)
	// Get kubernetes config.
	restConfig, err := clientcmd.BuildConfigFromFlags("", kubeConfig)
	if err != nil {
		log.Critical("Error building kubeconfig: %s", err.Error())
	}
	log.Debug("kube config: host: %s, APIPath: %s, Prefix: %s, user: %s",
		restConfig.Host, restConfig.APIPath, restConfig.Prefix, restConfig.Username)
	// create the clientset
	clientset, err = kubernetes.NewForConfig(restConfig)
	if err != nil {
		log.Critical("Failed to init rest config due to %v", err)
	} else {
		log.Info("connect to kube apiserver %s ok", restConfig.Host)
	}
}

func main() {
	// Call Parse() to avoid noisy logs
	flag.CommandLine.Parse([]string{})

	threadness := StringToInt(os.Getenv("THREADNESS"))

	initKubeClient()
	port := os.Getenv("PORT")
	if _, err := strconv.Atoi(port); err != nil {
		port = "39999"
	}

	// Set up signals so we handle the first shutdown signal gracefully.
	stopCh := signals.SetupSignalHandler()

	informerFactory := kubeinformers.NewSharedInformerFactory(clientset, resyncPeriod)
	controller, err := gpushare.NewController(clientset, informerFactory, stopCh)
	if err != nil {
		log.Critical("Failed to start due to %v", err)
	} else {
		log.Info("Create Controller ok")
	}
	err = controller.BuildCache()
	if err != nil {
		log.Critical("Failed to start due to %v", err)
	} else {
		log.Info("Build controller cache ok.")
	}

	go controller.Run(threadness, stopCh)

	go startWatchNodes(controller.GetSchedulerCache())

	//go startWatchPods(namespace, controller.GetSchedulerCache())

	gpusharePredicate := scheduler.NewGPUsharePredicate(clientset, controller.GetSchedulerCache())
	gpushareBind := scheduler.NewGPUShareBind(clientset, controller.GetSchedulerCache())
	gpushareInspect := scheduler.NewGPUShareInspect(controller.GetSchedulerCache())

	router := httprouter.New()

	routes.AddPProf(router)
	routes.AddVersion(router)
	routes.AddPredicate(router, gpusharePredicate)
	routes.AddBind(router, gpushareBind)
	routes.AddInspect(router, gpushareInspect)

	log.Info("Server starting on the port :%s", port)
	if err := http.ListenAndServe(":"+port, router); err != nil {
		log.Critical(err)
	}
}

func startWatchPods(namespace string, cache *cache.SchedulerCache) {
	log.Info("start watch ns %s's pods")
	w, err := clientset.CoreV1().Pods(namespace).Watch(metav1.ListOptions{LabelSelector: utils.ResourceName})
	if err != nil {
		log.Warning("watch ns %s's pods warning: %s", err)
	}
	for {
		select {
		case e, _ := <-w.ResultChan():
			switch e.Type {
			case watch.Added:
				log.Info("add %v", e.Object)
				err = cache.BuildPodsCache()
				CheckError("build cache warning: %s", err)
			case watch.Deleted:
				log.Info("delete %v", e.Object)
				err = cache.BuildPodsCache()
				CheckError("build cache warning: %s", err)
			case watch.Modified:
				log.Info("modify %v", e.Object)
				err = cache.BuildPodsCache()
				CheckError("build cache warning: %s", err)
			case watch.Error:
				log.Error("Error %v", e.Object)
			default:
				log.Info("event: %s %v", e.Type, e.Object)
			}
		}
	}
}
func CheckError(prefix string, err error) {
	if err != nil {
		log.Warning(prefix, err)
	}
}
func startWatchNodes(cache *cache.SchedulerCache) {
	log.Info("start watch nodes")
	w, err := clientset.CoreV1().Nodes().Watch(metav1.ListOptions{})
	if err != nil {
		log.Warning("watch nodes warning: %s", err)
	}
	for {
		select {
		case e, _ := <-w.ResultChan():
			if e.Object != nil {
				obj := e.Object
				node, ok := obj.(*v1.Node)
				if !ok {
					log.Warning("%v %v", e.Type, e.Object)
				} else {
					switch e.Type {
					case watch.Added:
						log.Info("add node %s", node.Name)
						err = cache.AddOrUpdateNode(node)
						CheckError("build cache warning: %s", err)
					case watch.Deleted:
						log.Info("delete node %s", node.Name)
						cache.RemoveNode(node)
					case watch.Modified:
						log.Info("modify %s", node.Name)
						//err = cache.AddOrUpdateNode(node)
						//CheckError("build cache warning: %s", err)
					case watch.Error:
						log.Error("Error %s", node.Name)
					default:
						log.Info("event: %s %s", e.Type, node.Name)
					}
				}
			}
		}
	}
}

func StringToInt(sThread string) int {
	thread := 1

	return thread
}
