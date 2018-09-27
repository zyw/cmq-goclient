package cmq

import (
	"testing"
	"fmt"
)

func TestClient_CmqCall(t *testing.T) {
	account := NewAccountDefault("https://cmq-queue-bj.api.qcloud.com", "", "")

	client := newCmqClient(account)
	params := map[string]interface{} {
		"queueName":"nsop-cloud-mq",
		"msgBody":"1111111111111111111",
	}
	result, err := client.cmqCall(SendMessage, params)

	if err != nil {
		fmt.Println("错误：" + err.Error())
		return
	}
	fmt.Println("结果：" + result)
}

func TestClient_CmqCall2(t *testing.T) {
	account := NewAccountDefault("https://cmq-queue-bj.api.qcloud.com", "", "")

	client := newCmqClient(account)
	params := map[string]interface{} {
		"queueName":"nsop-cloud-mq",
	}
	result, err := client.cmqCall(ReceiveMessage, params)

	if err != nil {
		fmt.Println("错误：" + err.Error())
		return
	}
	fmt.Println("结果：" + result)
}