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

func (this *Subscription) ClearFilterTags() error {

	params := map[string]interface{} {
		"topicName" : this.topicName,
		"subscriptionName" : this.subscriptionName,
	}

	return handleSubscriptionApi(this,ClearSUbscriptionFIlterTags,params)
}

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

	/*meta.FilterTag

	SubscriptionMeta meta = new SubscriptionMeta();
	meta.FilterTag = new Vector<String>();
	if(jsonObj.has("endpoint"))
		meta.Endpoint = jsonObj.getString("endpoint");
	if(jsonObj.has("notifyStrategy"))
		meta.NotifyStrategy = jsonObj.getString("notifyStrategy");
	if(jsonObj.has("notifyContentFormat"))
		meta.NotifyContentFormat = jsonObj.getString("notifyContentFormat");
	if(jsonObj.has("protocol"))
		meta.Protocal = jsonObj.getString("protocol");
	if(jsonObj.has("createTime"))
		meta.CreateTime = jsonObj.getInt("createTime");
	if(jsonObj.has("lastModifyTime"))
		meta.LastModifyTime = jsonObj.getInt("lastModifyTime");
	if(jsonObj.has("msgCount"))
		meta.msgCount = jsonObj.getInt("msgCount");
	if(jsonObj.has("filterTag"))
	{
		JSONArray jsonArray = jsonObj.getJSONArray("filterTag");
		for(int i=0;i<jsonArray.length();i++)
		{
			JSONObject obj = (JSONObject)jsonArray.get(i);
			meta.FilterTag.add(obj.toString());
		}
	}
	if(jsonObj.has("bindingKey"))
	{
		JSONArray jsonArray = jsonObj.getJSONArray("bindingKey");
		for(int i=0;i<jsonArray.length();i++)
		{
			JSONObject obj = (JSONObject)jsonArray.get(i);
			meta.bindingKey.add(obj.toString());
		}
	}*/

	return meta,nil
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