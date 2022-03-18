package worker

import (
	"encoding/json"
	"io/ioutil"
)

type Config struct{
	EtcdEndPoints []string `json:"etcdEndPoints"`
	EtcdDialTimeout int `json:"etcdDialTimeout"`
}

var (
	G_config *Config
)


func InitConfig(filename string)(err error){
	var (
		content []byte
		conf Config
	)
	//1.读配置文件
	if content,err = ioutil.ReadFile(filename);err !=nil{
		return
	}

	// 2. json反序列化
	if err  = json.Unmarshal(content,&conf);err!=nil{
		return
	}

	// 3.赋值单例
	G_config= &conf

	return

}
