package main

import (
	"flag"
	"fmt"
	log "github.com/astaxie/beego/logs"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/bnulwh/gpushare-scheduler-extender/pkg/gpushare"
	"github.com/bnulwh/gpushare-scheduler-extender/pkg/routes"
	"github.com/bnulwh/gpushare-scheduler-extender/pkg/scheduler"
	"github.com/bnulwh/gpushare-scheduler-extender/pkg/utils/signals"
	"github.com/julienschmidt/httprouter"

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

	// Get kubernetes config.
	restConfig, err := clientcmd.BuildConfigFromFlags("", kubeConfig)
	if err != nil {
		log.Critical("Error building kubeconfig: %s", err.Error())
	}

	// create the clientset
	clientset, err = kubernetes.NewForConfig(restConfig)
	if err != nil {
		log.Critical("fatal: Failed to init rest config due to %v", err)
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
	}
	err = controller.BuildCache()
	if err != nil {
		log.Critical("Failed to start due to %v", err)
	}

	go controller.Run(threadness, stopCh)

	gpusharePredicate := scheduler.NewGPUsharePredicate(clientset, controller.GetSchedulerCache())
	gpushareBind := scheduler.NewGPUShareBind(clientset, controller.GetSchedulerCache())
	gpushareInspect := scheduler.NewGPUShareInspect(controller.GetSchedulerCache())

	router := httprouter.New()

	routes.AddPProf(router)
	routes.AddVersion(router)
	routes.AddPredicate(router, gpusharePredicate)
	routes.AddBind(router, gpushareBind)
	routes.AddInspect(router, gpushareInspect)

	log.Info("info: server starting on the port :%s", port)
	if err := http.ListenAndServe(":"+port, router); err != nil {
		log.Critical(err)
	}
}

func StringToInt(sThread string) int {
	thread := 1

	return thread
}
