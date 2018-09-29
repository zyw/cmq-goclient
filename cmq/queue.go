package cmq

import (
	"encoding/json"
	"fmt"
	"errors"
	"strconv"
	"log"
)

type Queue struct {
	client *Client
	queueName string
}

// 设置队列属性
func (q *Queue) SetQueueAttributes(meta *QueueMeta) *CMQError {

	params := map[string]interface{} {
		"queueName": q.queueName,
	}

	if meta.maxMsgHeapNum > 0 {
		params["maxMsgHeapNum"] = meta.maxMsgHeapNum
	}

	if meta.pollingWaitSeconds > 0 {
		params["pollingWaitSeconds"] = meta.pollingWaitSeconds
	}

	if meta.visibilityTimeout > 0 {
		params["visibilityTimeout"] = meta.visibilityTimeout
	}

	if meta.maxMsgSize > 0 {
		params["maxMsgSize"] = meta.maxMsgSize
	}

	if meta.msgRetentionSeconds > 0 {
		params["msgRetentionSeconds"] = meta.msgRetentionSeconds
	}
	if meta.rewindSeconds > 0 {
		params["rewindSeconds"] = meta.rewindSeconds
	}
	return handleQueueApi(q,SetQueueAttributes,params)
}

//获取队列属性
func (q *Queue) GetQueueAttributes() (*QueueMeta,*CMQError) {
	params := map[string]interface{} {
		"queueName":q.queueName,
	}

	result, err := q.client.cmqCall(GetQueueAttributes, params)
	if err != nil {
		log.Println("create queue error msg: " + err.Error())
		return nil,err
	}

	var res map[string]interface{}
	if err := json.Unmarshal([]byte(result),&res);err != nil {
		log.Println("parse json string error, msg: " + err.Error())
		return nil,NewCMQOpError(CMQError102,jsonUnmarshal,GetQueueAttributes)
	}
	code := res["code"].(int)
	if code != 0 {
		log.Println(fmt.Sprintf("code:%d, %v, RequestId: %v",code,res["message"],res["requestId"]))
		return nil,NewCMQOpError(erron(code),errors.New(res["message"].(string)),GetQueueAttributes)
	}

	meta := &QueueMeta{
		maxMsgHeapNum:			res["maxMsgHeapNum"].(int),
		pollingWaitSeconds:		res["pollingWaitSeconds"].(int),
		visibilityTimeout:		res["visibilityTimeout"].(int),
		maxMsgSize:				res["maxMsgSize"].(int),
		msgRetentionSeconds:	res["msgRetentionSeconds"].(int),
		createTime:				res["createTime"].(int),
		lastModifyTime:			res["lastModifyTime"].(int),
		activeMsgNum:			res["activeMsgNum"].(int),
		inactiveMsgNum:			res["inactiveMsgNum"].(int),
		rewindmsgNum:			res["rewindMsgNum"].(int),
		minMsgTime:				res["minMsgTime"].(int),
		delayMsgNum:			res["delayMsgNum"].(int),
		rewindSeconds:			res["rewindSeconds"].(int),
	}

	return meta,nil;
}

func handleQueueApi(q *Queue,action string,params map[string]interface{}) *CMQError {
	result, err := q.client.cmqCall(action, params)
	if err != nil {
		log.Println("create queue error msg: " + err.Error())
		return err
	}

	var message msg
	if err := json.Unmarshal([]byte(result),&message);err != nil {
		log.Println("parse json string error, msg: " + err.Error())
		return NewCMQOpError(CMQError102,jsonUnmarshal,action)
	}
	code := message.Code
	if code != 0 {
		log.Println(fmt.Sprintf("code:%d, %v, RequestId: %v",code,message.Message,message.RequestId))
		return NewCMQOpError(erron(code),errors.New(message.Message),action)
	}
	return nil
}

// 发消息
// msgBody 消息正文。至少 1 Byte，最大长度受限于设置的队列消息最大长度属性。
// delaySeconds 单位为秒，表示该消息发送到队列后，需要延时多久用户才可见该消息。传0表示立即可见
func (q *Queue) SendMessage(msgBody string,delaySeconds int) (result string,err *CMQError) {

	if len(msgBody) == 0 {
		return "",NewCMQOpError(CMQError100,errors.New("msgBoy is empty!"),SendMessage)
	}

	if delaySeconds < 0 {
		return "",NewCMQOpError(CMQError100,errors.New("delaySeconds is < 0!"),SendMessage)
	}

	params := map[string]interface{} {
		"queueName":q.queueName,
		"msgBody":msgBody,
		"delaySeconds":delaySeconds,
	}

	r,err := q.client.cmqCall(SendMessage, params)

	if err != nil {
		return "",err
	}
	var message msg
	if err := json.Unmarshal([]byte(r),&message);err != nil {
		return "",NewCMQOpError(CMQError102,jsonUnmarshal,SendMessage)
	}
	code := message.Code
	if code != 0 {
		log.Println("parse json string error, msg: " + err.Error())
		return "",NewCMQOpError(erron(code),errors.New(message.Message),SendMessage)
	}

	return message.MsgId,nil
}

// 批量发送
// msgBodys 消息正文。表示这一批量中的一条消息。目前批量消息数量不能超过 16 条。
// 为方便用户使用，n从0开始或者从1开始都可以，但必须连续，例如发送两条消息，可以是(msgBody.0, msgBody.1)，或者(msgBody.1, msgBody.2)。
// 注意：由于目前限制所有消息大小总和（不包含消息头和其他参数，仅msgBody）不超过 64k，所以建议提前规划好批量发送的数量。
// delaySeconds 单位为秒，表示该消息发送到队列后，需要延时多久用户才可见。（该延时对一批消息有效，不支持多对多映射）
func (q *Queue) BatchSendMessage(msgBodys []string,delaySeconds int) (result []string,err *CMQError)  {

	if msgBodys == nil || len(msgBodys) == 0 || len(msgBodys) > 16 {
		return nil,NewCMQOpError(CMQError100,errors.New("Error: message size is empty or more than 16"),BatchSendMessage)
	}

	params := map[string]interface{} {
		"queueName":q.queueName,
		"delaySeconds":delaySeconds,
	}

	for i,v := range msgBodys {
		params["msgBody." + strconv.Itoa(i)] = v
	}

	r, err := q.client.cmqCall(BatchSendMessage, params)

	if err != nil {
		return nil,err
	}

	log.Println("result json: " + r)

	var message msg

	if err := json.Unmarshal([]byte(r),&message);err != nil {
		log.Println(fmt.Sprintf("code:%d, %v, RequestId: %v",message.Code,message.Message,message.RequestId))
		return nil,NewCMQOpError(CMQError102,jsonUnmarshal,BatchSendMessage)
	}

	if message.Code != 0 {
		return nil,NewCMQOpError(erron(message.Code),errors.New(message.Message),BatchSendMessage)
	}

	var res []string

	for _,v := range message.MsgList {
		res = append(res,v["msgId"])
	}

	return res,nil
}

//接受消息
// pollingWaitSeconds 本次请求的长轮询等待时间。取值范围 0-30 秒，如果不设置该参数，则默认使用队列属性中的 pollingWaitSeconds 值。
func (q *Queue) ReceiveMessage(pollingWaitSeconds int) (msg *Message,err *CMQError) {
	params := map[string]interface{} {
		"queueName":q.queueName,
	}
	if pollingWaitSeconds >=  0 {
		params["UserpollingWaitSeconds"] = pollingWaitSeconds + 3
		params["pollingWaitSeconds"] = pollingWaitSeconds
	} else {
		params["UserpollingWaitSeconds"] = 30
	}

	result, err := q.client.cmqCall(ReceiveMessage, params)
	if err != nil {
		return nil,err
	}

	var message Message

	if err := json.Unmarshal([]byte(result),&message);err != nil {
		return nil,NewCMQOpError(CMQError102,jsonUnmarshal,ReceiveMessage)
	}

	if message.Code != 0 {
		return nil,NewCMQOpError(erron(message.Code),errors.New(message.Message),ReceiveMessage)
	}

	return &message,nil;
}

// 批量接收消息
// numOfMsg               准备获取消息数
// pollingWaitSeconds     请求最长的Polling等待时间
func (q *Queue) BatchReceiveMessage(numOfMsg,pollingWaitSeconds int) (result []Message,err *CMQError) {

	params := map[string]interface{} {
		"queueName":q.queueName,
		"numOfMsg":numOfMsg,
	}
	if pollingWaitSeconds >= 0 {
		params["UserpollingWaitSeconds"] = pollingWaitSeconds + 3
		params["pollingWaitSeconds"] = pollingWaitSeconds
	} else {
		params["UserpollingWaitSeconds"] = 30
	}
	r, err := q.client.cmqCall(BatchReceiveMessage, params)

	if err != nil {
		return nil,err
	}
	log.Println("result json: " + r)

	var msg batchMessage

	if err := json.Unmarshal([]byte(r),&msg);err != nil {
		return nil,NewCMQOpError(CMQError102,jsonUnmarshal,BatchReceiveMessage)
	}

	if msg.Code != 0 {
		return nil,NewCMQOpError(erron(msg.Code),errors.New(msg.Message),BatchReceiveMessage)
	}

	msgs := make([]Message,len(msg.MsgInfoList))

	for i,v := range msg.MsgInfoList {
		msgs[i] = Message{
			Code:msg.Code,
			Message:msg.Message,
			RequestId:msg.RequestId,
			MsgId:v.MsgId,
			ReceiptHandle:v.ReceiptHandle,
			MsgBody:v.MsgBody,
			EnqueueTime:v.EnqueueTime,
			NextVisibleTime:v.NextVisibleTime,
			FirstDequeueTime:v.FirstDequeueTime,
			DequeueCount:v.DequeueCount,
		}
	}

	return msgs,nil
}

// 删除消息
// receiptHandle 上次消费返回唯一的消息句柄，用于删除消息。
func (q *Queue) DeleteMessage(receiptHandle string) *CMQError {

	params := map[string]interface{} {
		"queueName":q.queueName,
		"receiptHandle":receiptHandle,
	}

	result, err := q.client.cmqCall(DeleteMessage, params)

	if err != nil {
		return err
	}
	var message msg

	if err := json.Unmarshal([]byte(result),&message);err != nil {
		log.Println(err.Error())
		return NewCMQOpError(CMQError102,jsonUnmarshal,DeleteMessage)
	}
	if message.Code != 0 {
		return NewCMQOpError(erron(message.Code),errors.New(message.Message),DeleteMessage)
	}

	return nil
}

// 批量删除消息
// receiptHandle 上次消费返回唯一的消息句柄，用于删除消息。
func (q *Queue) BatchDeleteMessage(receiptHandles []string) *CMQError {

	if receiptHandles == nil || len(receiptHandles) == 0 {
		return NewCMQOpError(CMQError100,errors.New("receiptHandles is nil or empty!"),BatchDeleteMessage)
	}

	params := map[string]interface{} {
		"queueName":q.queueName,
	}

	for i,rh := range receiptHandles {
		params["receiptHandle." + strconv.Itoa(i)] = rh
	}

	result, err := q.client.cmqCall(BatchDeleteMessage, params)

	if err != nil {
		return err
	}
	var message msg

	if err := json.Unmarshal([]byte(result),&message);err != nil {
		log.Println(err.Error())
		return NewCMQOpError(CMQError102,jsonUnmarshal,BatchDeleteMessage)
	}
	if message.Code != 0 {
		return NewCMQOpError(erron(message.Code),errors.New(message.Message),BatchDeleteMessage)
	}

	return nil
}
