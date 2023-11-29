package main

import (
	"fmt"
	"time"
	"urchinfs/urchin"
)

func trySchedule(sourceURL, endpoint, bucket, objectKey, dstPeer string) {
	println("new request dstPeer: ", dstPeer)
	urfs := urchin.New()
	//scheduleResult, err := urfs.ScheduleDataToPeer(sourceURL, dstPeer)
	//if err != nil {
	//	println(err.Error())
	//}
	//fmt.Printf("ScheduleDataToPeer StatusCode:%v %v %v %v\n", scheduleResult.StatusCode, scheduleResult.DataEndpoint, scheduleResult.DataRoot, scheduleResult.DataPath)
	//
	//checkResult, err := urfs.CheckScheduleTaskStatus(sourceURL, dstPeer)
	//if err != nil {
	//	println(err.Error())
	//}
	//
	//fmt.Printf("checkResult StatusCode:%v %v %v %v\n", checkResult.StatusCode, checkResult.DataEndpoint, checkResult.DataRoot, checkResult.DataPath)

	overwrite := true
	scheduleResult, err := urfs.ScheduleDataToPeerByKey(endpoint, bucket, objectKey, dstPeer, overwrite)
	if err != nil {
		println(err.Error())
	}
	fmt.Printf("ScheduleDataToPeerByKey StatusCode:%v %v %v %v\n", scheduleResult.StatusCode, scheduleResult.DataEndpoint, scheduleResult.DataRoot, scheduleResult.DataPath)

	time.Sleep(time.Second * 2)

	scheduleResult, err = urfs.CheckScheduleTaskStatusByKey(endpoint, bucket, objectKey, dstPeer)
	if err != nil {
		println(err.Error())
	}
	fmt.Printf("CheckScheduleTaskStatusByKey StatusCode:%v StatusMsg:%v\n", scheduleResult.StatusCode, scheduleResult.StatusMsg)
}

func tryScheduleDir(endpoint, bucket, objectKey, dstPeer string) {
	println("new request dstPeer: ", dstPeer)
	urfs := urchin.New()

	//scheduleResult, err := urfs.ScheduleDirToPeerByKey(endpoint, bucket, objectKey, dstPeer)
	//if err != nil {
	//	print(err.Error())
	//	return
	//}

	//fmt.Printf("ScheduleDataToPeerByKey StatusCode:%v %v %v %v\n", scheduleResult.StatusCode, scheduleResult.DataEndpoint, scheduleResult.DataRoot, scheduleResult.DataPath)

	scheduleResult, err := urfs.CheckScheduleDirTaskStatusByKey(endpoint, bucket, objectKey, dstPeer)
	if err != nil {
		print(err.Error())
		return
	}
	fmt.Printf("CheckScheduleTaskStatusByKey StatusCode:%v %v %v %v\n", scheduleResult.StatusCode, scheduleResult.DataEndpoint, scheduleResult.DataRoot, scheduleResult.DataPath)
}

func main() {

	sourceURL := "urfs://obs.cn-south-222.ai.pcl.cn/urchincache/glin/demo_x/object_detection3/code/openi_resource.version"
	endpoint := "obs.cn-central-231.xckpjs.com"
	bucket := "urchincache"
	objectKey := "glin/demo_x/object_detection3/code/openi_resource.version"
	dstPeer := "192.168.242.42:31814"
	trySchedule(sourceURL, endpoint, bucket, objectKey, dstPeer)
	//sourceURL2 := "urfs://11276.c8befbc1301665ba2dc5b2826f8dca1e.ac.sugon.com/work-home-denglf-denglf/code.rar"
	//endpoint2 := "obs.cn-south-222.ai.pcl.cn"

	//bucket2 := "urchincache"
	//objectKey2 := "yangxzh/object-detection-image.zip"
	//dstPeer2 := "192.168.207.91:65004"

	//bucket2 := "open-data"
	//objectKey2 := "attachment/9/6/96177b0c-6f84-4550-b293-09c206baf811/MNISTData.zip"
	//dstPeer2 := "192.168.207.91:65004"

	//dstPeer2 := "192.168.242.25:65004"
	//trySchedule(sourceURL2, endpoint2, bucket2, objectKey2, dstPeer2)
	//endpoint := "192.168.242.23:31311"
	//bucket := "grampus"
	//objectKey := "job/wangj2023031409t5509373540/output/"
	//dstPeer := "192.168.242.27:65004"
	// schedule dir example
	//endpoint := "192.168.242.23:31311"
	//bucket := "grampus"
	//objectKey := "/job/cheny2023030215t5435897690/output"
	//dstPeer := "192.168.242.42:65004"
	//tryScheduleDir(endpoint, bucket, objectKey, dstPeer)

}
