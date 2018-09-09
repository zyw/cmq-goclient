package cmq

import (
	"testing"
	"fmt"
	"strconv"
)

func TestQueue_SendMessage(t *testing.T) {
	account := NewAccountDefault("https://cmq-queue-bj.api.qcloud.com", "", "")

	queue := account.GetQueue("nsop-cloud-mq")
	result, err := queue.SendMessage("4444444444444444444", 0)
	if err != nil {
		fmt.Println("错误：" + err.Error())
		return
	}
	fmt.Println("结果：" + result)
}

func TestCmqConfig_GetQueue(t *testing.T) {
	account := NewAccountDefault("https://cmq-queue-bj.api.qcloud.com", "", "")

	queue := account.GetQueue("nsop-cloud-mq")

	message, err := queue.ReceiveMessage(30)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println(strconv.Itoa(message.Code) + "  " + message.MsgId + " 内容：" + message.MsgBody)
}

func TestQueue_BatchSendMessage(t *testing.T) {
	account := NewAccountDefault("https://cmq-queue-bj.api.qcloud.com", "", "")

	queue := account.GetQueue("nsop-cloud-mq")

	result, err := queue.BatchSendMessage([]string{"aaa", "bbbb"}, 0)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	fmt.Println(result)
}

func TestQueue_BatchReceiveMessage(t *testing.T) {
	account := NewAccountDefault("https://cmq-queue-bj.api.qcloud.com", "", "")

	queue := account.GetQueue("nsop-cloud-mq")

	result, err := queue.BatchReceiveMessage(2, 0)

	if err != nil {
		fmt.Println(err.Error())
		return
	}

	fmt.Println(result)
}