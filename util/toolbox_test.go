package util

import (
	"testing"
	"fmt"
)

func TestHmacSHA256(t *testing.T) {
	mw := HmacSHA256("GETcvm.api.qcloud.com/v2/index.php?Action=DescribeInstances&InstanceIds.0=ins-09dx96dg&Nonce=11886&Region=ap-guangzhou&SecretId=AKIDz8krbsJ5yKBZQpn74WFkmLPx3gnPhESA&SignatureMethod=HmacSHA256&Timestamp=1465185768",
		"Gu5t9xGARNpq86cd98joQYCN3Cozk1qA")
	fmt.Println(Base64(mw))
}

func TestMapToURLParam(t *testing.T) {
	param := map[string]interface{} {
		"Action" : "DescribeInstances",
		"Nonce" : 11886,
		"InstanceIds.0" : "ins-09dx96dg",
		"SecretId" : "AKIDz8krbsJ5yKBZQpn74WFkmLPx3gnPhESA",
		"Region" : "ap-guangzhou",

		"SignatureMethod" : "HmacSHA256",
		"Timestamp" : 1465185768,
	}
	fmt.Println(MapToURLParam(param,false))
}