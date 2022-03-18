package main

import (
	"crontab/master"
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
	flag.StringVar(&confFile,"config","./master.json","指定master.json")
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
	if err = master.InitConfig(confFile);err!=nil{
		goto ERR
	}
	//任务管理器
	if err = master.InitJobMgr();err!=nil{
		goto ERR
	}

	// 启动 HTTP服务
	if err = master.InitApiServer(); err!=nil{
		goto ERR
	}

	// 主线程不退
	for {
		time.Sleep(1*time.Second)
	}

ERR:
	fmt.Println(err)
}




