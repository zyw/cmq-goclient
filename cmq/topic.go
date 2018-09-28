package cmq

import (
	"github.com/pkg/errors"
	"log"
	"encoding/json"
	"fmt"
	"strconv"
)

const (
	SetTopicAttributes 			=	"SetTopicAttributes"
	GetTopicAttributes			=	"GetTopicAttributes"
	PublishMessage				=	"PublishMessage"
	BatchPublishMessage			=	"BatchPublishMessage"
)

type Topic struct {
	topicName string
	client *Client
}

type TopicMeta struct {
	// 当前该主题的消息堆积数
	msgCount 				int
	// 消息最大长度，取值范围1024-1048576 Byte（即1-1024K），默认1048576
	maxMsgSize				int
	//消息在主题中最长存活时间，从发送到该主题开始经过此参数指定的时间后，
	//不论消息是否被成功推送给用户都将被删除，单位为秒。固定为一天，该属性不能修改。
	msgRetentionSeconds		int
	//创建时间
	createTime				int
	//修改属性信息最近时间
	lastModifyTime			int
	loggingEnabled			int
	filterType				int
}

func (t *Topic) SetTopicAttributes(maxMsgSize int) *CMQError {
	if maxMsgSize < 1024 || maxMsgSize > 1048576 {
		return NewCMQOpError(CMQError100,errors.New("Invalid parameter maxMsgSize < 1KB or maxMsgSize > 1024KB"),SetTopicAttributes)
	}

	params := map[string]interface{} {
		"topicName":t.topicName,
		"maxMsgSize":maxMsgSize,
	}

	return handleTopicApi(t,SetTopicAttributes,params)
}

func (t *Topic) GetTopicAttributes() (*TopicMeta,*CMQError) {
	params := map[string]interface{} {
		"topicName":t.topicName,
	}
	result, err := t.client.cmqCall(GetTopicAttributes, params)
	if err != nil {
		return nil,err
	}
	var res map[string]interface{}
	if err := json.Unmarshal([]byte(result),&res);err != nil {
		log.Println("parse json string error, msg: " + err.Error())
		return nil,NewCMQOpError(CMQError102,jsonUnmarshal,GetTopicAttributes)
	}
	code := res["code"].(int)
	if code != 0 {
		log.Println(fmt.Sprintf("code:%d, %v, RequestId: %v",code,res["message"],res["requestId"]))
		return nil,NewCMQOpError(erron(code),errors.New(res["message"].(string)),GetTopicAttributes)
	}

	return &TopicMeta{
		msgCount:res["msgCount"].(int),
		maxMsgSize:res["maxMsgSize"].(int),
		msgRetentionSeconds:res["msgRetentionSeconds"].(int),
		createTime:res["createTime"].(int),
		lastModifyTime:res["lastModifyTime"].(int),
		filterType:res["filterType"].(int),
	},nil
}

//发布消息
//topicName 主题名字，在单个地域同一帐号下唯一。主题名称是一个不超过 64 个字符的字符串，必须以字母为首字符，剩余部分可以包含字母、数字和横划线(-)。
//msgBody 消息正文。至少 1 Byte，最大长度受限于设置的主题消息最大长度属性。
//msgTag.n 消息过滤标签。消息标签（用于消息过滤)。标签数量不能超过 5 个，每个标签不超过 16 个字符。与 Subscribe 接口的 filterTag 参数配合使用，规则：
//（1）1）如果 filterTag 没有设置，则无论 msgTag是 否有设置，订阅接收所有发布到 Topic 的消息；
//（2）如果 filterTag 数组有值，则只有数组中至少有一个值在 msgTag 数组中也存在时（即 filterTag 和 msgTag 有交集），订阅才接收该发布到 Topic 的消息；
//（3）如果 filterTag 数组有值，但 msgTag 没设置，则不接收任何发布到 Topic 的消息，可以认为是（2）的一种特例，此时 filterTag 和 msgTag 没有交集。规则整体的设计思想是以订阅者的意愿为主。
//routingKey 长度<=64字节，该字段用于表示发送消息的路由路径，最多含有 15 个“.”，即最多 16 个词组。
//消息发送到 topic 类型的 exchange 上时不能随意指定 routingKey。需要符合上面的格式要求，一个由订阅者指定的带有 routingKey 的消息将会推送给所有 BindingKey 能与之匹配的消费者，这种匹配情况有两种关系：
//1 *（星号），可以替代一个单词（一串连续的字母串）；
//2 #（井号）：可以匹配一个或多个字符。
func (t *Topic) PublishMessage(message string, vTagList []string,routingKey string) (string,*CMQError) {
	params := map[string]interface{} {
		"topicName": t.topicName,
		"msgBody": message,
	}

	if len(routingKey) > 0 {
		params["routingKey"] = routingKey
	}

	for i,tl := range vTagList {
		params["msgTag."+strconv.Itoa(i+1)] = tl
	}
	result, err := t.client.cmqCall(PublishMessage, params)
	if err != nil {
		log.Println("create queue error msg: " + err.Error())
		return "",err
	}

	var m msg
	if err := json.Unmarshal([]byte(result),&m);err != nil {
		log.Println("parse json string error, msg: " + err.Error())
		return "",NewCMQOpError(CMQError102,jsonUnmarshal,PublishMessage)
	}
	code := m.Code
	if code != 0 {
		log.Println(fmt.Sprintf("code:%d, %v, RequestId: %v",code,m.Message,m.RequestId))
		return "",NewCMQOpError(erron(code),errors.New(m.Message),PublishMessage)
	}

	return m.MsgId,nil
}

func (t *Topic) BatchPublishMessage(vMsgList,vTagList []string,routingKey string) ([]string,*CMQError){

	params := map[string]interface{} {
		"topicName":t.topicName,
	}

	if len(routingKey) !=0 {
		params["routingKey"] = routingKey
	}

	for i,msg := range vTagList {
		params["msgBody." + strconv.Itoa(i+1)] = msg
	}

	for i,tl := range vTagList {
		params["msgTag."+strconv.Itoa(i+1)] = tl
	}

	result, err := t.client.cmqCall(BatchPublishMessage, params)

	if err != nil {
		log.Println("create queue error msg: " + err.Error())
		return nil,err
	}

	var m msg
	if err := json.Unmarshal([]byte(result),&m);err != nil {
		log.Println("parse json string error, msg: " + err.Error())
		return nil,NewCMQOpError(CMQError102,jsonUnmarshal,BatchPublishMessage)
	}
	code := m.Code
	if code != 0 {
		log.Println(fmt.Sprintf("code:%d, %v, RequestId: %v",code,m.Message,m.RequestId))
		return nil,NewCMQOpError(erron(code),errors.New(m.Message),BatchPublishMessage)
	}

	var list = make([]string,len(m.MsgList))
	for i,m := range m.MsgList {
		list[i] = m["msgId"]
	}

	return list,nil
}

func handleTopicApi(topic *Topic,action string,params map[string]interface{}) *CMQError {
	result, err := topic.client.cmqCall(action, params)
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
		return NewCMQOpError(erron(message.Code),errors.New(message.Message),action)
	}
	return nil
}