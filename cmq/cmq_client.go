package cmq

import (
	"time"
	"math/rand"
	"strings"
	"io/ioutil"
	"errors"
	"net/http"
	"github.com/zyw/cmq-goclient/util"
)

const (
	//发送消息Action
	SendMessage 			= "SendMessage"
	//获取消息Action
	ReceiveMessage 			= "ReceiveMessage"
	//批量发送消息Action
	BatchSendMessage 		= "BatchSendMessage"
	//批量消费消息Action
	BatchReceiveMessage		= "BatchReceiveMessage"
	//删除消息Action
	DeleteMessage 			= "DeleteMessage"
	//批量删除消息
	BatchDeleteMessage		= "BatchDeleteMessage"
	SetQueueAttributes		= "SetQueueAttributes"
	GetQueueAttributes		= "GetQueueAttributes"
)

type Message struct {
	Code int					`json:"code"` 			//0：表示成功，others：错误，详细错误见下表。
	Message string				`json:"message"`			//错误提示信息。
	RequestId string			`json:"requestId"`    	//服务器生成的请求 Id，出现服务器内部错误时，用户可提交此 Id 给后台定位问题。
	MsgId string				`json:"msgId"`    		//(单条)服务器生成消息的唯一标识 Id。
	ReceiptHandle string		`json:"receiptHandle"`	// 每次消费唯一的消息句柄，用于删除等操作
	MsgBody string				`json:"msgBody"`			// 消息体
	EnqueueTime int64			`json:"enqueueTime"`		// 消息发送到队列的时间，从 1970年1月1日 00:00:00 000 开始的毫秒数
	NextVisibleTime int64		`json:"nextVisibleTime"`	// 消息下次可见的时间，从 1970年1月1日 00:00:00 000 开始的毫秒数
	FirstDequeueTime int64		`json:"firstDequeueTime"`	// 消息第一次出队列的时间，从 1970年1月1日 00:00:00 000 开始的毫秒数
	DequeueCount int			`json:"dequeueCount"`		// 出队列次数
	MsgTag []string				`json:"msgTag"`
}

type msg struct {
	Code int					`json:"code"` 		//0：表示成功，others：错误，详细错误见下表。
	Message string				`json:"message"`		//错误提示信息。
	RequestId string			`json:"requestId"`    //服务器生成的请求 Id，出现服务器内部错误时，用户可提交此 Id 给后台定位问题。
	MsgId string				`json:"msgId"`    	//(单条)服务器生成消息的唯一标识 Id。
	MsgList []map[string]string	`json:"msgList"`		//(批量)服务器生成消息的唯一标识 Id 列表，每个元素是一条消息的信息。
}

type batchMessage struct {
	Code 			int				`json:"code"`
	Message 		string 			`json:"message"`
	RequestId 		string			`json:"requestId"`
	ClientRequestId int				`json:"clientRequestId"`
	MsgInfoList 	[]msgInfoList	`json:"msgInfoList"`
}
type msgInfoList struct {
	MsgId string				`json:"msgId"`
	ReceiptHandle string		`json:"receiptHandle"`
	MsgBody string				`json:"msgBody"`
	EnqueueTime int64			`json:"enqueueTime"`
	NextVisibleTime int64		`json:"nextVisibleTime"`
	FirstDequeueTime int64 		`json:"firstDequeueTime"`
	DequeueCount int			`json:"dequeueCount"`
}

type CmqConfig struct {
	currentVersion string
	endpoint string
	path string
	secretId string
	secretKey string
	method string
	signMethod string
}

func NewAccountDefault(endpoint, secretId, secretKey string) *CmqConfig  {
	return &CmqConfig{
		currentVersion:"SDK_GOLANG_1.0",
		endpoint:endpoint,
		path:"/v2/index.php",
		secretId:secretId,
		secretKey:secretKey,
		method:"POST",
		signMethod:"sha256",
	}
}
func NewAccount(endpoint, secretId, secretKey,method,signMethod string) *CmqConfig  {
	return &CmqConfig{
		currentVersion:"SDK_GOLANG_1.0",
		endpoint:endpoint,
		path:"/v2/index.php",
		secretId:secretId,
		secretKey:secretKey,
		method:method,
		signMethod:signMethod,
	}
}

//返回Queue对象
//读取队列消息，向队列发送消息有关
func (a *CmqConfig) GetQueue(queueName string) *Queue  {
	return &Queue{
		client:newCmqClient(a),
		queueName:queueName,
	}
}
//创建CMQ
//和创建主题，队列，订阅有关
func (a *CmqConfig) GetCmq() *Cmq {
	return &Cmq{
		client:newCmqClient(a),
	}
}
//创建Topic
//修改主题参数，向主题发送消息
func (a *CmqConfig) GetTopic(topicName string) *Topic {
	return &Topic{
		topicName:topicName,
		client:newCmqClient(a),
	}
}
//获取订阅
//订阅参数设置
func (a *CmqConfig) GetSubscription(topicName,subscriptionName string) *Subscription {
	return &Subscription{
		topicName:topicName,
		subscriptionName:subscriptionName,
		client:newCmqClient(a),
	}
}

type Client struct {
	account *CmqConfig
}

func newCmqClient(account *CmqConfig) *Client {
	return &Client{
		account:account,
	}
}

// 调用CMQ API完成操作，比如：发送消息读取消息，创建队列创建主题
func (cc *Client) cmqCall(action string,params map[string]interface{}) (result string,e *CMQError)  {
	if len(action) == 0 {
		return "",NewCMQOpError(CMQError100,errors.New("action param is Zero value"),action)
	}
	if params == nil || len(params) == 0 {
		return "",NewCMQOpError(CMQError100,errors.New("params is nil or len = 0"),action)
	}

	params["Action"] = action
	params["Nonce"] = rand.Int()
	params["SecretId"] = cc.account.secretId
	params["Timestamp"] = time.Now().Unix()
	params["RequestClient"] = cc.account.currentVersion



	if cc.account.signMethod =="sha256" {
		params["SignatureMethod"] = "HmacSHA256"
	} else {
		params["SignatureMethod"] = "HmacSHA1"
	}

	var host string
	if strings.HasPrefix(cc.account.endpoint,"https") {
		host = cc.account.endpoint[8:]
	} else {
		host = cc.account.endpoint[7:]
	}

	src := cc.account.method + host + cc.account.path + "?" + util.MapToURLParam(params,false)

	params["Signature"] = util.Base64(util.HmacSHA256(src,cc.account.secretKey))
	var url string
	var param string
	if cc.account.method == "GET" {
		url = cc.account.endpoint + cc.account.path + "?" + util.MapToURLParam(params,true)
		if len(url) > 2048 {
			return "",NewCMQOpError(CMQError100,errors.New("URL length is larger than 2K when use GET method"),action)
		}
	} else {
		url = cc.account.endpoint + cc.account.path
		param = util.MapToURLParam(params,true)
	}
	var userTimeout int
	if _,ok := params["UserpollingWaitSeconds"];ok {
		userTimeout = params["UserpollingWaitSeconds"].(int)
	}

	r,err := httpRequest(cc.account.method,url,param,userTimeout)

	if err != nil {
		return "",err
	}

	return r,nil
}

func httpRequest(method,url,param string,timeout int) (result string,e *CMQError) {
	client := &http.Client{
		Timeout: time.Duration(timeout) * time.Second,
	}
	req, err := http.NewRequest(method, url, strings.NewReader(param))
	if err != nil {
		return "",NewCMQError(CMQError1011,err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := client.Do(req)

	if err != nil {
		return "",NewCMQError(CMQError1012,err)
	}

	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)

	if err != nil {
		return "",NewCMQError(CMQError1013,err)
	}
	return string(body),nil
}