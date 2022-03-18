package worker

import (
	"context"
	"crontab/common"
	"go.etcd.io/etcd/api/mvccpb"
	"go.etcd.io/etcd/client/v3"
	"time"
)

// 任务管理器
type JobMgr struct{
	client *clientv3.Client
	kv clientv3.KV
	lease clientv3.Lease
	watcher clientv3.Watcher

}

var (
	G_jobMgr *JobMgr
)


// 初始化任务管理器
// 通过etcd 获取任务信息
func InitJobMgr()(err error){
	var (
		client *clientv3.Client
		watcher clientv3.Watcher
	)
	//初始化配置
	config := clientv3.Config{
		Endpoints:G_config.EtcdEndPoints,
		DialTimeout:time.Duration(G_config.EtcdDialTimeout)*time.Millisecond,
	}
	if client,err = clientv3.New(config);err!=nil{
		return
	}
	kv := clientv3.NewKV(client)
	lease := clientv3.NewLease(client)
	watcher = clientv3.NewWatcher(client)

	G_jobMgr = &JobMgr{
		client: client,
		kv:     kv,
		lease:  lease,
		watcher: watcher,
	}

	// 启动监听
	G_jobMgr.watchJobs()

	//启动监听killer


	return
}


//监听任务变化
func(jobMgr *JobMgr)watchJobs()(err error){
	var (
		getResp *clientv3.GetResponse
		kvpair *mvccpb.KeyValue
		job *common.Job
		watchStartRevision int64
		watchChan clientv3.WatchChan
		watchResp clientv3.WatchResponse
		watchEvent *clientv3.Event
		jobName string
		jobEvent *common.JobEvent
	)

	// 1. /cron/jobs/目录下所有任务
	if getResp,err = jobMgr.kv.Get(context.TODO(),common.JOB_SAVE_DIR,clientv3.WithPrefix());err!=nil{
		return
	}

	//当前有哪些任务
	// etcd中已有的任务 打上状态 推给scheduler
	for _,kvpair = range getResp.Kvs{
		//反序列化
		if job,err = common.UnpackJob(kvpair.Value);err!=nil{
			// 给job打上状态然后job同步给scheduler
			jobEvent = common.BuildJobEvent(common.JOB_EVENT_SAVE,job)
			//fmt.Println(*jobEvent)
			G_scheduler.PushJobEvent(jobEvent)
		}
	}

	// 2.从该revision向后监听变化事件
	// 监听协程  给job打上状态 推给scheduler
	go func(){
		//从GET时刻的后续版本开始监听变化
		watchStartRevision = getResp.Header.Revision +1
		// 监听/cron/jobs/目录的后续变化
		watchChan = jobMgr.watcher.Watch(context.TODO(),common.JOB_SAVE_DIR,clientv3.WithRev(watchStartRevision),clientv3.WithPrefix())

		//处理监听事件
		for watchResp = range watchChan{
			for _,watchEvent = range watchResp.Events{
				switch watchEvent.Type {
					case mvccpb.PUT: //任务保存事件
						if job,err = common.UnpackJob(watchEvent.Kv.Value);err!=nil{
							continue
						}
						//构建一个更新Event, 推给scheduler
						jobEvent = common.BuildJobEvent(common.JOB_EVENT_SAVE,job)
					case mvccpb.DELETE: //任务被删除了
						// Delete /cron/jobs/job10
						jobName = common.ExtractJobName(string(watchEvent.Kv.Key)) //提取job名
						job = &common.Job{Name: jobName}
						//构建一个删除Event
						jobEvent = common.BuildJobEvent(common.JOB_EVENT_DELETE,job)
				}
				//fmt.Println(*jobEvent)
				G_scheduler.PushJobEvent(jobEvent)
			}
		}  //end_for

	}()
	return
}

// 监听强杀任务通知
func(jobMgr *JobMgr)watchKiller()(err error){
	var (
		watchChan clientv3.WatchChan
		watchResp clientv3.WatchResponse
		watchEvent *clientv3.Event
		jobName string
		jobEvent *common.JobEvent
		job *common.Job
	)

	// 监听/cron/killer目录
	go func(){ // 监听协程
		// 监听/cron/killer/目录的变化
		watchChan = jobMgr.watcher.Watch(context.TODO(),common.JOB_KILLER_DIR,clientv3.WithPrefix())
		//处理监听事件
		for watchResp = range watchChan{
			for _,watchEvent = range watchResp.Events{
				switch watchEvent.Type {
					case mvccpb.PUT: //任务保存事件
						jobName = common.ExtractKillerName(string(watchEvent.Kv.Key))
						job = &common.Job{Name:jobName}
						jobEvent = common.BuildJobEvent(common.JOB_EVENT_KILL,job)
						//事件推给scheduler
						G_scheduler.PushJobEvent(jobEvent)
					case mvccpb.DELETE: //killer标记过期,被自动删除
				}
			}
		}
	}()
	return
}

// 创建任务执行锁
func(jobMgr *JobMgr)CreateJobLock(jobName string)(jobLock *JobLock){
	jobLock = InitJobLock(jobName,jobMgr.kv,jobMgr.lease)
	return
}




