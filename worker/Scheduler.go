package worker

import (
	"crontab/common"
	"fmt"
	"time"
)

// 任务调度
type Scheduler struct{
	jobEventChan chan *common.JobEvent  // etcd 任务事件队列
	jobPlanTable map[string]*common.JobSchedulePlan //任务调度计划表
	jobExecutingTable map[string]*common.JobExecuteInfo //任务执行表
	jobResultChan chan *common.JobExecuteResult  //任务结果队列
}


var (
	G_scheduler *Scheduler
)

// 初始化调度器
func InitScheduler()(err error){
	G_scheduler = &Scheduler{
		jobEventChan: make(chan *common.JobEvent,1000),
		jobPlanTable: make(map[string]*common.JobSchedulePlan),
		jobExecutingTable: make(map[string]*common.JobExecuteInfo),
		jobResultChan: make(chan *common.JobExecuteResult,1000),
	}
	//启动调度协程
	go G_scheduler.scheduleLoop()
	return
}

// 推送任务变化事件
func (scheduler *Scheduler)PushJobEvent(jobEvent *common.JobEvent){
	scheduler.jobEventChan <-jobEvent
}

// 处理任务事件
func (scheduler *Scheduler)handleJoEvent(jobEvent *common.JobEvent){
	var(
		jobSchedulePlan *common.JobSchedulePlan
		err error
		jobExisted bool
		jobExisting bool
		jobExecuteInfo *common.JobExecuteInfo
	)
	switch jobEvent.EventType{
		case common.JOB_EVENT_SAVE://保存任务事件
			// 任务调度计划（expr解析好了）
			if jobSchedulePlan,err = common.BuildJobSchedulePlan(jobEvent.Job);err!=nil{
				return
			}
			scheduler.jobPlanTable[jobEvent.Job.Name] = jobSchedulePlan
		case common.JOB_EVENT_DELETE://删除任务事件
			if jobSchedulePlan,jobExisted = scheduler.jobPlanTable[jobEvent.Job.Name];jobExisted{
				delete(scheduler.jobPlanTable,jobEvent.Job.Name)
			}
		case common.JOB_EVENT_KILL: //强杀任务事件
			//取消掉Command执行,判断任务是否在执行中
			if jobExecuteInfo,jobExisting = scheduler.jobExecutingTable[jobEvent.Job.Name];jobExisting{
				jobExecuteInfo.CancelFunc()  //触发command杀死shell子进程,任务得到退出
			}
	}
}

// 处理任务结果
func(scheduler *Scheduler)handleJobResult(result *common.JobExecuteResult){
	// 删除执行状态
	delete(scheduler.jobExecutingTable,result.ExecuteInfo.Job.Name)

	fmt.Println("任务执行完成:",result.ExecuteInfo.Job.Name,string(result.Output),result.Err)
}

// 尝试执行任务
func (scheduler *Scheduler)TryStartJob(jobPlan *common.JobSchedulePlan){
	// 调度 和 执行
	var (
		jobExecuteInfo *common.JobExecuteInfo
		jobExecuting bool
	)
	// 执行的任务可能运行很久,一分钟会调度60次，但是只能执行一次，防止并发
	// 如果任务正在执行，跳过本次调度
	if jobExecuteInfo,jobExecuting = scheduler.jobExecutingTable[jobPlan.Job.Name];jobExecuting{
		return
	}

	// 填充执行状态信息
	jobExecuteInfo = common.BuildJobExecuteInfo(jobPlan)

	// 保存执行状态
	scheduler.jobExecutingTable[jobPlan.Job.Name] = jobExecuteInfo

	// 执行任务
	//fmt.Println("执行任务:",jobExecuteInfo.Job.Name,jobExecuteInfo.PlanTime,jobExecuteInfo.RealTime)
	G_executor.ExecuteJob(jobExecuteInfo)
}

// 重新计算任务调度状态
func(scheduler *Scheduler)TrySchedule()(schedulerAfter time.Duration){
	var (
		jobPlan *common.JobSchedulePlan
		now time.Time
		nearTime *time.Time
	)

	// 如果任务表为空, 睡眠一下
	if len(scheduler.jobPlanTable) == 0{
		schedulerAfter = 1* time.Second
		return
	}

	// 当前时间
	now = time.Now()

	//遍历所有任务
	for _,jobPlan = range scheduler.jobPlanTable{
		if jobPlan.NextTime.Before(now) || jobPlan.NextTime.Equal(now){
			// 执行任务
			//fmt.Println("执行任务",jobPlan.Job.Name)
			scheduler.TryStartJob(jobPlan)
			jobPlan.NextTime = jobPlan.Expr.Next(now)  //更新下次执行时间
		}

		// 统计最近一个要过期的任务时间
		if nearTime == nil || jobPlan.NextTime.Before(*nearTime){
			nearTime = &jobPlan.NextTime
		}
	}

	// 下次调度间隔( 最近要执行的任务调度时间 - 当前时间 )
	schedulerAfter = (*nearTime).Sub(now)
	return
}

// 调度协程
func (scheduler *Scheduler)scheduleLoop(){
	var (
		jobEvent *common.JobEvent
		scheduleAfter time.Duration
		scheduleTimer *time.Timer
		jobResult *common.JobExecuteResult
	)

	// 初始化一次(1秒)
	scheduleAfter = scheduler.TrySchedule()

	//调度的延迟定时器
	scheduleTimer = time.NewTimer(scheduleAfter)

	//定时任务
	for{
		select {
			case jobEvent=<-scheduler.jobEventChan:  //监听任务变化事件
			//对内存中维护的任务列表做增删改查
			scheduler.handleJoEvent(jobEvent)
			case <- scheduleTimer.C: //最近的任务到期了
			case jobResult = <-scheduler.jobResultChan:  //监听任务执行结果
			scheduler.handleJobResult(jobResult)
		}
		//调度一次任务
		scheduleAfter = scheduler.TrySchedule()
		//重置调度间隔
		scheduleTimer.Reset(scheduleAfter)
	}
}

// 回传任务执行结果
func(scheduler *Scheduler)PushJobResult(jobResult *common.JobExecuteResult){
	scheduler.jobResultChan<-jobResult
}


