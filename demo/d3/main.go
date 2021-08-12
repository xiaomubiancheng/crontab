package main

import (
	"context"
	"fmt"
	"os/exec"
	"time"
)

type result struct{
	output []byte
	err error
}

func main() {
	var (
		ctx        context.Context
		cancelFunc context.CancelFunc
		cmd        *exec.Cmd
		resultChan chan *result
		res  *result
	)

	resultChan = make(chan *result, 1000)

	ctx, cancelFunc = context.WithCancel(context.TODO())

	go func() {
		var (
			output []byte
			err    error
		)

		cmd = exec.CommandContext(ctx, "C:\\Tools\\git\\Git\\bin\\bash.exe", "-c", "sleep 2;echo hello;")
		output, err = cmd.CombinedOutput()

		resultChan <- &result{
			err:err,
			output: output,
		}
	}()

	time.Sleep(1* time.Second)

	//取消上下文
	cancelFunc()


	res = <- resultChan

	// 打印任务,执行结果
	fmt.Println(res.err, string(res.output))
}
