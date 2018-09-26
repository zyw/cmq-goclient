package cmq

import (
	"strings"
	"log"
	"errors"
	"fmt"
	"encoding/json"
	"strconv"
)

type Cmq struct {
	client *Client
}

const (
	// 缺省消息接收长轮询等待时间
	DefaultPollingWaitSeconds 	= 	0
	// 缺省消息可见性超时
	DefaultVisibilityTimeout 	= 	30
	// 缺省消息最大长度，单位字节
	DefaultMaxMsgSize 			= 	1048576
	// 缺省消息保留周期，单位秒
	DefaultMsgRetentionSeconds 	= 	345600
	//创建队列Action
	CreateQueue					=	"CreateQueue"
	//删除队列Action
	DeleteQueue 				= 	"DeleteQueue"
	//队列列表Action
	ListQueue					=	"ListQueue"
	//创建Topic Action
	CreateTopic					=	"CreateTopic"
	//删除Topic Action
	DeleteTopic					=	"DeleteTopic"
	//Topic列表 Action
	ListTopic					=	"ListTopic"
	//创建订阅 Action
	Subscribe					=	"Subscribe"
	//删除订阅
	Unsubscribe					=	"Unsubscribe"
)

type ListQueueResult struct {
	Code int				`json:"code"`
	Message string			`json:"message"`
	RequestId string 		`json:"requestId"`
	TotalCount int 			`json:"totalCount"`
	QueueList []QueueList 	`json:"queueList"`
}

type QueueList struct {
	//队列的唯一标识Id。
	QueueId string			`json:"queueId"`
	//队列名字，在单个地域同个帐号下唯一。 队列名称是一个不超过 64 个字符的字符串，必须以字母为首字符，剩余部分可以包含字母、数字和横划线(-)。
	QueueName string		`json:"queueName"`
}

type ListTopicResult struct {
	Code int				`json:"code"`
	Message string			`json:"message"`
	RequestId string 		`json:"requestId"`
	TotalCount int 			`json:"totalCount"`
	TopicList []TopicList 	`json:"topicList"`
}

type TopicList struct {
	TopicId string			`json:"topicId"`
	TopicName string 		`json:"topicName"`
}

type QueueMeta struct {
	/** 最大堆积消息数 */
	maxMsgHeapNum int
	/** 消息接收长轮询等待时间 */
	pollingWaitSeconds int
	/** 消息可见性超时 */
	visibilityTimeout int
	/** 消息最大长度 */
	maxMsgSize int
	/** 消息保留周期 */
	msgRetentionSeconds int
	/** 队列创建时间 */
	createTime int
	/** 队列属性最后修改时间 */
	lastModifyTime int
	/** 队列处于Active状态的消息总数 */
	activeMsgNum int
	/** 队列处于Inactive状态的消息总数 */
	inactiveMsgNum int

	/** 已删除的消息，但还在回溯保留时间内的消息数量 */
	rewindmsgNum int
	/** 消息最小未消费时间 */
	minMsgTime int
	/** 延时消息数量 */
	delayMsgNum int

	/** 回溯时间 */
	rewindSeconds int
}

func NewDefaultQueueMeta() *QueueMeta {
	return &QueueMeta{
		maxMsgHeapNum:-1,
		pollingWaitSeconds:DefaultPollingWaitSeconds,
		visibilityTimeout:DefaultVisibilityTimeout,
		maxMsgSize:DefaultMaxMsgSize,
		msgRetentionSeconds:DefaultMsgRetentionSeconds,
		createTime:-1,
		lastModifyTime:-1,
		activeMsgNum:-1,
		inactiveMsgNum:-1,
	}
}

func (meta *QueueMeta) SetMaxMsgHeapNum(maxMsgHeapNum int)  {
	meta.maxMsgHeapNum = maxMsgHeapNum
}

func (meta *QueueMeta) SetPollingWaitSeconds(pollingWaitSeconds int)  {
	meta.pollingWaitSeconds = pollingWaitSeconds
}

func (meta *QueueMeta) SetVisibilityTimeout(visibilityTimeout int)  {
	meta.visibilityTimeout = visibilityTimeout
}

func (meta *QueueMeta) SetMaxMsgSize(maxMsgSize int)  {
	meta.maxMsgSize = maxMsgSize
}

func (meta *QueueMeta) SetMsgRetentionSeconds(msgRetentionSeconds int)  {
	meta.msgRetentionSeconds = msgRetentionSeconds
}

func (meta *QueueMeta) SetCreateTime(createTime int)  {
	meta.createTime = createTime
}

func (meta *QueueMeta) SetLastModifyTime(lastModifyTime int)  {
	meta.lastModifyTime = lastModifyTime
}

func (meta *QueueMeta) SetActiveMsgNum(activeMsgNum int)  {
	meta.activeMsgNum = activeMsgNum
}

func (meta *QueueMeta) SetInactiveMsgNum(inactiveMsgNum int)  {
	meta.inactiveMsgNum = inactiveMsgNum
}

func (meta *QueueMeta) SetRewindmsgNum(rewindmsgNum int)  {
	meta.rewindmsgNum = rewindmsgNum
}

func (meta *QueueMeta) SetMinMsgTime(minMsgTime int)  {
	meta.minMsgTime = minMsgTime
}

func (meta *QueueMeta) SetDelayMsgNum(delayMsgNum int)  {
	meta.delayMsgNum = delayMsgNum
}

func (meta *QueueMeta) SetRewindSeconds(rewindSeconds int)  {
	meta.rewindSeconds = rewindSeconds
}

// 创建队列
func (cmq *Cmq) CreateQueue(queueName string,meta *QueueMeta) error {
	qn := strings.TrimSpace(queueName)
	if len(qn) == 0 {
		log.Println("Invalid parameter:queueName is empty")
		return errors.New("Invalid parameter:queueName is empty")
	}
	params := map[string]interface{} {
		"queueName":qn,
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
	return handleCmqApi(cmq,CreateQueue, params)
}
// 删除队列
func (cmq *Cmq) DeleteQueue(queueName string) error {
	qn := strings.TrimSpace(queueName)
	if len(qn) == 0 {
		log.Println("Invalid parameter:queueName is empty")
		return errors.New("Invalid parameter:queueName is empty")
	}

	params := map[string]interface{} {
		"queueName":qn,
	}
	return handleCmqApi(cmq,DeleteQueue,params)
}

// 队列列表
// searchWord 用于过滤队列列表，后台用模糊匹配的方式来返回符合条件的队列列表。如果不填该参数，默认返回帐号下的所有队列。
// offset 分页时本页获取队列列表的起始位置。如果填写了该值，必须也要填写 limit 。该值缺省时，后台取默认值 0
// limit 分页时本页获取队列的个数，如果不传递该参数，则该参数默认为 20，最大值为 50。
// queueList 引用类型，存放查询到的queue列表，保存queueName
func (cmq *Cmq) ListQueue(searchWord string,offset,limit int, queueList []string ) (int,error) {

	params := map[string]interface{}{}

	if len(searchWord) != 0 {
		params["searchWord"] = searchWord
	}

	if offset >= 0 {
		params["offset"] = offset
	}

	if limit > 0 {
		params["limit"] = limit
	}

	result, err := cmq.client.cmqCall(ListQueue, params)

	if err != nil {
		return 0,err
	}

	var res ListQueueResult
	if err := json.Unmarshal([]byte(result),&res);err != nil {
		log.Println("parse json string error, msg: " + err.Error())
		return 0,errors.New("parse json string error!")
	}
	if res.Code != 0 {
		log.Println(fmt.Sprintf("code:%d, %v, RequestId: %v",res.Code,res.Message,res.RequestId))
		return 0,errors.New(fmt.Sprintf("code:%d, %v, RequestId: %v",res.Code,res.Message,res.RequestId))
	}

	for i,qs := range res.QueueList {
		queueList[i] = qs.QueueName
	}

	return res.TotalCount,nil
}

//创建Topic
// topicName 主题名字，在单个地域同一帐号下唯一。主题名称是一个不超过 64 个字符的字符串，必须以字母为首字符，剩余部分可以包含字母、数字和横划线(-)。
// maxMsgSize 消息最大长度。取值范围 1024-65536 Byte（即1-64K），默认值 65536。
// filterType：
// 		用于指定主题的消息匹配策略：
//		filterType =1 或为空， 表示该主题下所有订阅使用 filterTag 标签过滤；
//		filterType =2 表示用户使用 bindingKey 过滤。
//		注：该参数设定之后不可更改。
func (cmq *Cmq) CreateTopic(topicName string,maxMsgSize,filterType int) error {
	tn := strings.TrimSpace(topicName)

	if len(tn) == 0 {
		return errors.New("Invalid parameter:topicName is empty")
	}

	if maxMsgSize < 1024 || maxMsgSize > 1048576 {
		return errors.New("Invalid parameter: maxMsgSize > 1024KB or maxMsgSize < 1KB")
	}

	params := map[string]interface{} {
		"topicName":tn,
		"filterType":filterType,
		"maxMsgSize":maxMsgSize,
	}

	return handleCmqApi(cmq,CreateTopic,params)
}

func (cmq *Cmq) DeleteTopic(topicName string) error {
	tn := strings.TrimSpace(topicName)
	if len(tn) == 0 {
		return errors.New("Invalid parameter:topicName is empty")
	}

	params := map[string]interface{} {
		"topicName":tn,
	}

	return handleCmqApi(cmq,DeleteTopic,params)
}

// Topic list
// searchWord 用于过滤主题列表，后台用模糊匹配的方式来返回符合条件的主题列表。如果不填该参数，默认返回帐号下的所有主题。
// offset 分页时本页获取主题列表的起始位置。如果填写了该值，必须也要填写 limit 。该值缺省时，后台取默认值 0
// limit 分页时本页获取主题的个数，如果不传递该参数，则该参数默认为 20，最大值为 50。
func (cmq *Cmq) ListTopic(searchWord string, vTopicList []string ,offset,limit int) (int,error) {
	params := map[string]interface{}{}

	if len(searchWord) != 0 {
		params["searchWord"] = searchWord
	}
	if offset >=0 {
		params["offset"] = offset
	}
	if limit > 0 {
		params["limit"] = limit
	}
	result, err := cmq.client.cmqCall(ListTopic, params)

	if err != nil {
		return 0,err
	}

	var res ListTopicResult
	if err := json.Unmarshal([]byte(result),&res);err != nil {
		log.Println("parse json string error, msg: " + err.Error())
		return 0,errors.New("parse json string error!")
	}
	if res.Code != 0 {
		log.Println(fmt.Sprintf("code:%d, %v, RequestId: %v",res.Code,res.Message,res.RequestId))
		return 0,errors.New(fmt.Sprintf("code:%d, %v, RequestId: %v",res.Code,res.Message,res.RequestId))
	}

	for i,ts := range res.TopicList {
		vTopicList[i] = ts.TopicName
	}

	return res.TotalCount,nil
}
// 创建订阅
// topicName 主题名字，在单个地域同一帐号下唯一。主题名称是一个不超过 64 个字符的字符串，必须以字母为首字符，剩余部分可以包含字母、数字和横划线(-)。
// subscriptionName 订阅名字，在单个地域同一帐号的同一主题下唯一。订阅名称是一个不超过 64 个字符的字符串，必须以字母为首字符，剩余部分可以包含字母、数字和横划线(-)。
// protocol 订阅的协议，目前支持两种协议：http、queue。使用 http 协议，用户需自己搭建接受消息的 web server。使用 queue，消息会自动推送到 CMQ queue，用户可以并发地拉取消息。
// endpoint 接收通知的 endpoint，根据协议 protocol 区分：对于 http，endpoint 必须以 “http://” 开头，host 可以是域名或 IP；对于 queue，则填 queueName。 请注意，目前推送服务不能推送到私有网络中，因此 endpoint 填写为私有网络域名或地址将接收不到推送的消息，目前支持推送到公网和基础网络。
// notifyStrategy 向 endpoint 推送消息出现错误时，CMQ 推送服务器的重试策略。取值有：1）BACKOFF_RETRY，退避重试。每隔一定时间重试一次，重试够一定次数后，就把该消息丢弃，继续推送下一条消息；2）EXPONENTIAL_DECAY_RETRY，指数衰退重试。每次重试的间隔是指数递增的，例如开始 1s，后面是2s，4s，8s...由于 Topic 消息的周期是一天，所以最多重试一天就把消息丢弃。默认值是EXPONENTIAL_DECAY_RETRY。
// notifyContentFormat 推送内容的格式。取值：1）JSON；2）SIMPLIFIED，即 raw 格式。如果 protocol 是 queue，则取值必须为 SIMPLIFIED。如果 protocol 是 http，两个值均可以，默认值是 JSON。
// filterTag.n 消息正文。消息标签（用于消息过滤)。标签数量不能超过5个，每个标签不超过16个字符。与 (Batch)PublishMessage 的 msgTag 参数配合使用，规则：1）如果 filterTag 没有设置，则无论 msgTag 是否有设置，订阅接收所有发布到 Topic 的消息；2）如果 filterTag 数组有值，则只有数组中至少有一个值在 msgTag 数组中也存在时（即 filterTag 和 msgTag 有交集），订阅才接收该发布到 Topic 的消息；3）如果 filterTag 数组有值，但 msgTag 没设置，则不接收任何发布到 Topic 的消息，可以认为是2）的一种特例，此时 filterTag 和 msgTag 没有交集。规则整体的设计思想是以订阅者的意愿为主。
// bindingKey.n bindingKey 数量不超过 5 个， 每个 bindingKey 长度不超过 64 字节，该字段表示订阅接收消息的过滤策略，每个 bindingKey 最多含有 15 个“.”， 即最多 16 个词组。
func (cmq *Cmq) CreateSubscribe(topicName,subscriptionName,endpoint,protocal string,
	filterTag, bindingKey []string,
	notifyStrategy,notifyContentFormat string) error {

	tn := strings.TrimSpace(topicName)
	if len(tn) == 0 {
		return errors.New("Invalid parameter:topicName is empty")
	}

	ssn := strings.TrimSpace(subscriptionName)
	if len(ssn) == 0 {
		return errors.New("Invalid parameter:subscriptionName is empty")
	}

	ep := strings.TrimSpace(endpoint)
	if len(ep) == 0 {
		return errors.New("Invalid parameter:endpoint is empty")
	}
	p := strings.TrimSpace(protocal)
	if len(p) == 0 {
		return errors.New("Invalid parameter:protocal is empty")
	}

	ns := strings.TrimSpace(notifyStrategy)
	if len(ns) == 0 {
		return errors.New("Invalid parameter:NotifyStrategy is empty")
	}

	ncf := strings.TrimSpace(notifyContentFormat)
	if len(ncf) == 0 {
		return errors.New("Invalid parameter:NotifyContentFormat is empty")
	}

	if len(filterTag) > 5 {
		return errors.New("Invalid parameter: Tag number > 5")
	}

	params := map[string]interface{} {
		"topicName":tn,
		"subscriptionName":ssn,
		"endpoint":ep,
		"protocol":p,
		"notifyStrategy":ns,
		"notifyContentFormat":ncf,
	}

	for i,ft := range filterTag {
		"filterTag."+ strconv.Itoa(i+1) = ft
	}
	for i,bk := range bindingKey {
		"bindingKey."+ strconv.Itoa(i+1) = bk
	}

	return handleCmqApi(cmq,Subscribe,params)
}
// 删除订阅
// topicName 主题名字，在单个地域同一帐号下唯一。主题名称是一个不超过 64 个字符的字符串，必须以字母为首字符，剩余部分可以包含字母、数字和横划线(-)。
// subscriptionName 订阅名字，在单个地域同一帐号的同一主题下唯一。订阅名称是一个不超过 64 个字符的字符串，必须以字母为首字符，剩余部分可以包含字母、数字和横划线(-)。
func (cmq *Cmq) DeleteSubscribe(topicName,subscriptionName string) error {
	tn := strings.TrimSpace(topicName)
	if len(tn) == 0 {
		return errors.New("Invalid parameter:topicName is empty")
	}

	ssn := strings.TrimSpace(subscriptionName)
	if len(ssn) == 0 {
		return errors.New("Invalid parameter:subscriptionName is empty")
	}

	params := map[string]interface{} {
		"topicName":topicName,
		"subscriptionName":subscriptionName,
	}

	return handleCmqApi(cmq,Unsubscribe,params)
}

func handleCmqApi(cmq *Cmq,action string,params map[string]interface{}) error {
	result, err := cmq.client.cmqCall(action, params)
	if err != nil {
		log.Println("create queue error msg: " + err.Error())
		return err
	}

	var message msg
	if err := json.Unmarshal([]byte(result),&message);err != nil {
		log.Println("parse json string error, msg: " + err.Error())
		return errors.New("parse json string error!")
	}
	code := message.Code
	if code != 0 {
		log.Println(fmt.Sprintf("code:%d, %v, RequestId: %v",code,message.Message,message.RequestId))
		return errors.New(fmt.Sprintf("code:%d, %v, RequestId: %v",code,message.Message,message.RequestId))
	}
	return nil
}