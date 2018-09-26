package cmq

import (
	"github.com/labstack/gommon/log"
	"encoding/json"
	"fmt"
	"errors"
	"strconv"
)

const (
	NotifyStrategyDefault 				= 	"BACKOFF_RETRY"
	NotifyContentFormatDefault			=	"JSON"
	ClearSUbscriptionFIlterTags 		=	"ClearSUbscriptionFIlterTags"
	SetSubscriptionAttributes			=	"SetSubscriptionAttributes"
	GetSubscriptionAttributes			=	"GetSubscriptionAttributes"
	ListSubscriptionByTopic				=	"ListSubscriptionByTopic"
)

type Subscription struct {
	topicName string
	subscriptionName string
	client *Client
}

type SubscriptionMeta struct {
	//Subscription 订阅的主题所有者的appId
	TopicOwner 			string
	//订阅的终端地址
	Endpoint			string
	//订阅的协议
	Protocal			string
	//推送消息出现错误时的重试策略
	NotifyStrategy		string
	//向 Endpoint 推送的消息内容格式
	NotifyContentFormat	string
	//描述了该订阅中消息过滤的标签列表（仅标签一致的消息才会被推送）
	FilterTag			[]string
	//Subscription 的创建时间，从 1970-1-1 00:00:00 到现在的秒值
	CreateTime			int
	//修改 Subscription 属性信息最近时间，从 1970-1-1 00:00:00 到现在的秒值
	LastModifyTime		int
	//该订阅待投递的消息数
	MsgCount			int
	BindingKey			[]string
}

type SubscriptionResult struct {
	Code int								`json:"code"`
	Message string							`json:"message"`
	RequestId string 						`json:"requestId"`
	TotalCount int 							`json:"totalCount"`
	SubscriptionList []SubscriptionList 	`json:"subscriptionList"`
}

type SubscriptionList struct {
	SubscriptionId	string		`json:"subscriptionId"`
	SubscriptionName string		`json:"subscriptionName"`
	Protocol		 string		`json:"protocol"`
	Endpoint		 string		`json:"endpoint"`
}

func (this *Subscription) ClearFilterTags() error {

	params := map[string]interface{} {
		"topicName" : this.topicName,
		"subscriptionName" : this.subscriptionName,
	}

	return handleSubscriptionApi(this,ClearSUbscriptionFIlterTags,params)
}

// 修改订阅属性
func (this *Subscription) SetSubscriptionAttributes(meta SubscriptionMeta) error {
	params := map[string]interface{} {
		"topicName" : this.topicName,
		"subscriptionName" : this.subscriptionName,
	}

	if len(meta.NotifyStrategy) != 0 {
		params["notifyStrategy"] = meta.NotifyStrategy
	}

	if len(meta.NotifyContentFormat) != 0 {
		params["notifyContentFormat"] = meta.NotifyContentFormat
	}

	for i,ft := range meta.FilterTag {
		params["filterTag." + strconv.Itoa(i+1)] = ft
	}

	for i,bk := range meta.BindingKey {
		params["bindingKey." + strconv.Itoa(i+1)] = bk
	}

	return handleSubscriptionApi(this,SetSubscriptionAttributes,params)
}

// 获取订阅属性
func (this *Subscription) GetSubscriptionAttributes() (*SubscriptionMeta,error) {

	params := map[string]interface{} {
		"topicName" : this.topicName,
		"subscriptionName" : this.subscriptionName,
	}

	result, err := this.client.cmqCall(GetSubscriptionAttributes, params)
	if err != nil {
		log.Error("create queue error msg: " + err.Error())
		return nil,err
	}
	var res map[string]interface{}
	if err := json.Unmarshal([]byte(result),&res);err != nil {
		log.Error("parse json string error, msg: " + err.Error())
		return nil,errors.New("parse json string error!")
	}
	code := res["code"].(int)
	if code != 0 {
		log.Error(fmt.Sprintf("code:%d, %v, RequestId: %v",code,res["message"],res["requestId"]))
		return nil,errors.New(fmt.Sprintf("code:%d, %v, RequestId: %v",code,res["message"],res["requestId"]))
	}

	var meta *SubscriptionMeta

	endpoint,ok := res["endpoint"].(string)
	if ok && len(endpoint) != 0 {
		meta.Endpoint = endpoint
	}

	notifyStrategy,ok := res["notifyStrategy"].(string)
	if ok && len(notifyStrategy) != 0 {
		meta.NotifyStrategy = notifyStrategy
	}

	notifyContentFormat,ok := res["notifyContentFormat"].(string)
	if ok && len(notifyContentFormat) !=0 {
		meta.NotifyContentFormat = notifyContentFormat
	}

	protocol,ok := res["protocol"].(string)
	if ok && len(protocol) != 0 {
		meta.Protocal = protocol
	}

	createTime,ok := res["createTime"].(int)
	if ok {
		meta.CreateTime = createTime
	}

	lastModifyTime,ok := res["lastModifyTime"].(int)
	if ok {
		meta.LastModifyTime = lastModifyTime
	}

	msgCount,ok := res["msgCount"].(int)
	if ok {
		meta.MsgCount = msgCount
	}

	fts,ok := res["filterTag"].([]string)
	if ok && fts != nil && len(fts) != 0 {
		for i,ft := range fts {
			meta.FilterTag[i] = ft
		}
	}

	bks,ok := res["bindingKey"].([]string)
	if ok && bks != nil && len(bks) != 0 {
		for i,bk := range bks {
			meta.BindingKey[i] = bk
		}
	}

	return meta,nil
}
// 获取订阅列表
// searchWord 用于过滤订阅列表，后台用模糊匹配的方式来返回符合条件的订阅列表。如果不填该参数，默认返回帐号下的所有订阅。
// offset 分页时本页获取订阅列表的起始位置。如果填写了该值，必须也要填写 limit。该值缺省时，后台取默认值 0。取值范围 0-1000。
// limit 分页时本页获取订阅的个数，该参数取值范围 0-100。如果不传递该参数，则该参数默认为 20。
func (this *Subscription) ListSubscription(offset,limit int,searchWord string,vSubscriptionList []string) (int,error) {
	params := map[string]interface{} {
		"topicName":this.topicName,
	}
	if len(searchWord) != 0 {
		params["searchWord"] = searchWord
	}
	if offset >= 0 {
		params["offset"] = offset
	}
	if limit >= 0 {
		params["limit"] = limit
	}
	result, err := this.client.cmqCall(ListSubscriptionByTopic, params)
	if err != nil {
		log.Error("create queue error msg: " + err.Error())
		return 0,err
	}

	var sr SubscriptionResult
	if err := json.Unmarshal([]byte(result),&sr);err != nil {
		log.Error("parse json string error, msg: " + err.Error())
		return 0,errors.New("parse json string error!")
	}

	code := sr.Code
	if code != 0 {
		log.Error(fmt.Sprintf("code:%d, %v, RequestId: %v",code,sr.Message,sr.RequestId))
		return 0,errors.New(fmt.Sprintf("code:%d, %v, RequestId: %v",code,sr.Message,sr.RequestId))
	}

	for i,sl := range sr.SubscriptionList {
		vSubscriptionList[i] = sl.SubscriptionName
	}

	return sr.TotalCount,nil
}

func handleSubscriptionApi(sub *Subscription,action string,params map[string]interface{}) error {
	result, err := sub.client.cmqCall(action, params)
	if err != nil {
		log.Error("create queue error msg: " + err.Error())
		return err
	}

	var message msg
	if err := json.Unmarshal([]byte(result),&message);err != nil {
		log.Error("parse json string error, msg: " + err.Error())
		return errors.New("parse json string error!")
	}
	code := message.Code
	if code != 0 {
		log.Error(fmt.Sprintf("code:%d, %v, RequestId: %v",code,message.Message,message.RequestId))
		return errors.New(fmt.Sprintf("code:%d, %v, RequestId: %v",code,message.Message,message.RequestId))
	}
	return nil
}