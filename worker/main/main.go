package main

import (
	"crontab/worker"
	"flag"
	"fmt"
	"runtime"
	"time"
)

var (
	confFile string //配置文件路径
)

//解析命令行参数
func initArgs(){
	flag.StringVar(&confFile,"config","./worker.json","指定master.json")
	flag.Parse()
}

// 初始化线程数量
func initEnv(){
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func main(){
	var (
		err error
	)

	//初始化命令行参数
	initArgs()

	//初始化线程
	initEnv()

	//加载配置
	if err = worker.InitConfig(confFile);err!=nil{
		goto ERR
	}

	// 执行器
	if err = worker.InitExecutor();err!=nil{
		goto ERR
	}

	//启动调度器
	if err = worker.InitScheduler();err!=nil{
		goto ERR
	}

	//任务管理器
	if err = worker.InitJobMgr();err!=nil{
		goto ERR
	}




	for {
		time.Sleep(1*time.Second)
	}

	return

ERR:
	fmt.Println(err)
}




