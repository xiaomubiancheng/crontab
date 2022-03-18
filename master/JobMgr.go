package master

import (
	"context"
	"crontab/common"
	"encoding/json"
	"go.etcd.io/etcd/api/mvccpb"
	"go.etcd.io/etcd/client/v3"
	"time"
)

// 任务管理器
type JobMgr struct{
	client *clientv3.Client
	kv clientv3.KV
	lease clientv3.Lease

}

var (
	G_jobMgr *JobMgr
)


func InitJobMgr()(err error){

	var (
		client *clientv3.Client
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

	G_jobMgr = &JobMgr{
		client: client,
		kv:     kv,
		lease:  lease,
	}

	return
}

// 保存任务
func (jobMgr *JobMgr)SaveJob(job *common.Job)(oldJob *common.Job,err error){
	var (
		jobKey string
		jobValue []byte
		putResp *clientv3.PutResponse
		oldJobObj common.Job
	)
	// etcd保存的key
	jobKey = common.JOB_SAVE_DIR + job.Name
	// 任务信息json
	if jobValue,err = json.Marshal(job);err!=nil{
		return
	}
	// 保存到etcd   k:v
	if putResp,err = jobMgr.kv.Put(context.TODO(),jobKey,string(jobValue),clientv3.WithPrevKV());err!=nil{
		return
	}
	//更新返回旧值
	if putResp.PrevKv!=nil{
		if err = json.Unmarshal(putResp.PrevKv.Value,&oldJobObj);err!=nil{
			err = nil
			return
		}
		oldJob = &oldJobObj
	}
	return
}


// 删除任务
func(jobMgr *JobMgr)DeleteJob(name string)(oldJob *common.Job,err error){
	var (
		oldJobObj common.Job
		jobKey string
		delResp *clientv3.DeleteResponse
	)
	jobKey = common.JOB_SAVE_DIR+name
	if delResp,err = jobMgr.kv.Delete(context.TODO(),jobKey,clientv3.WithPrevKV());err !=nil{
		return
	}
	//返回被删除的任务信息
	if len(delResp.PrevKvs)!=0{
		if err = json.Unmarshal(delResp.PrevKvs[0].Value, &oldJobObj);err!=nil{
			err = nil
			return
		}
		oldJob = &oldJobObj
	}
	return
}

// 任务列表
func(jobMgr *JobMgr)ListJobs()(jobs []*common.Job,err error){
	var (
		dirKey string
		getResp *clientv3.GetResponse
		job *common.Job
		kvPair *mvccpb.KeyValue
	)

	//任务保存目录
	dirKey = common.JOB_SAVE_DIR

	//获取目录下所有的任务信息
	if getResp,err = jobMgr.kv.Get(context.TODO(),dirKey,clientv3.WithPrefix());err!=nil{
		return
	}

	// 初始化数组空间
	jobs = make([]*common.Job,0)

	//遍历所有的任务,进行反序列化
	for _,kvPair=range getResp.Kvs{
		job = &common.Job{}
		if err = json.Unmarshal(kvPair.Value,job);err!=nil{
			err = nil
			continue
		}
		jobs = append(jobs,job)
	}
	return
}

// 杀死任务
func(jobMgr *JobMgr)KillJob(name string)(err error){
	var (
		killerKey string
		leaseGrantResp *clientv3.LeaseGrantResponse
		leaseId clientv3.LeaseID
	)

	killerKey = common.JOB_KILLER_DIR +name

	//让worker监听到一次put操作,创建一个租约让其稍后自动过期
	if leaseGrantResp,err = jobMgr.lease.Grant(context.TODO(),1);err!=nil{
		return
	}

	leaseId = leaseGrantResp.ID
	// 设置killer标记
	if _,err = jobMgr.kv.Put(context.TODO(),killerKey,"",clientv3.WithLease(leaseId));err!=nil{
		return
	}
	return
}