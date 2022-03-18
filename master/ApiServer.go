package master

import (
	"crontab/common"
	"encoding/json"
	"net"
	"net/http"
	"strconv"
	"time"
)

type ApiServer struct {
	httpServer *http.Server
}

var (
	//单例对象
	G_apiServer *ApiServer
)


func InitApiServer()(err error){
	var (
		listener net.Listener
		staticDir http.Dir
		staticHandler http.Handler
	)

	//配置路由
	mux := http.NewServeMux()
	mux.HandleFunc("/job/save",handleJobSave)
	mux.HandleFunc("/job/delete",handleJoBDelete)
	mux.HandleFunc("/job/list",handleJoBList)
	mux.HandleFunc("/job/kill",handleJobKill)

	//静态页面
	staticDir = http.Dir(G_config.Web)
	staticHandler = http.FileServer(staticDir)
	mux.Handle("/", http.StripPrefix("/",staticHandler))

	//启动TCP监听
	if listener,err = net.Listen("tcp",":"+strconv.Itoa(G_config.ApiPort));err!=nil{
		return err
	}

	//创建HTTP服务
	httpServer := &http.Server{
		ReadTimeout: time.Duration(G_config.ApiReadTimeout)*time.Millisecond,
		WriteTimeout: time.Duration(G_config.ApiWriteTimeout)*time.Millisecond,
		Handler: mux,
	}

	// 赋值单例
	G_apiServer = &ApiServer{
		httpServer: httpServer,
	}

	//启动服务
	go httpServer.Serve(listener)
	return
}

// 任务列表
func handleJoBList(w http.ResponseWriter, r *http.Request){
	var (
		jobs []*common.Job
		err error
		bytes []byte
	)
	//获取任务列表
	if jobs,err = G_jobMgr.ListJobs();err!=nil{
		goto ERR
	}
	if bytes,err = common.BuildResponse(0,"success",jobs);err==nil{
		w.Write(bytes)
	}
	return
ERR:
	if bytes,err = common.BuildResponse(-1,err.Error(),nil);err==nil{
		w.Write(bytes)
	}

}


// 保存任务
// POST job={"name":"job1","command":"echo hello","cronExpr":"*/5 * * * * * *"}
func handleJobSave(w http.ResponseWriter, r *http.Request){
	var (
		err error
		postJob string
		job common.Job
		oldJob *common.Job
		bytes []byte
	)
	// 1.解析POST表单
	if err =r.ParseForm();err!=nil{
		goto ERR
	}
	//2.取表单中的job字段
	postJob = r.PostForm.Get("job")
	// 3.反序列化job
	if err = json.Unmarshal([]byte(postJob),&job);err!=nil{
		goto ERR
	}
	// 4.保存到etcd
	if oldJob,err = G_jobMgr.SaveJob(&job);err!=nil{
		goto ERR
	}
	// 5.返回正常应答({"error":0,"msg":"","data":{...}})
	if bytes,err = common.BuildResponse(0,"success",oldJob);err==nil{
		w.Write(bytes)
	}
	return

ERR:
	//6.返回异常应答
	if bytes,err = common.BuildResponse(-1,err.Error(),nil);err ==nil{
		w.Write(bytes)
	}
}

// 删除任务
func handleJoBDelete(w http.ResponseWriter, r *http.Request){
	var (
		err error
		jobName string
		oldJob *common.Job
		bytes []byte
	)

	if err = r.ParseForm();err!=nil{
		goto ERR
	}

	jobName = r.PostForm.Get("name")

	if oldJob,err = G_jobMgr.DeleteJob(jobName);err!=nil{
		goto ERR
	}
	if bytes,err = common.BuildResponse(0,"success",oldJob);err == nil{
		w.Write(bytes)
	}
	return

ERR:
	if bytes,err = common.BuildResponse(-1,err.Error(),nil);err==nil{
		w.Write(bytes)
	}
}


// 强杀任务
func handleJobKill(w http.ResponseWriter, r *http.Request){
	var (
		err error
		name string
		bytes []byte
	)
	if err = r.ParseForm();err!=nil{
		goto ERR
	}
	name = 	r.PostForm.Get("name")

	if err = G_jobMgr.KillJob(name);err!=nil{
		goto ERR
	}

	if bytes,err = common.BuildResponse(0,"success",nil);err==nil{
		w.Write(bytes)
	}
	return

ERR:
	if bytes,err = common.BuildResponse(-1,err.Error(),nil);err==nil{
		w.Write(bytes)
	}
}


