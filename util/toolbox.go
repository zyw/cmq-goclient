package util

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"sort"
	"strings"
	"strconv"
	"net/url"
	"fmt"
)

func HmacSHA256(plaintext, secret string) []byte {
	h := hmac.New(sha256.New,[]byte(secret))
	h.Write([]byte(plaintext))

	return h.Sum(nil)
}

func Base64(src []byte) string  {
	return base64.StdEncoding.EncodeToString(src)
}

func MapToURLParam(src map[string]interface{},encoder bool) string {
	var keys []string
	for k,_ := range src {
		keys = append(keys,k)
	}

	sort.Strings(keys)

	param := make([]string,len(keys))

	for i,k := range keys {
		key := strings.Replace(k,"_",".",-1)
		if s,ok := src[k].(string);ok {
			if encoder {
				param[i] = key + "=" + url.QueryEscape(s)
			} else {
				param[i] = key + "=" + s
			}
		} else if s,ok := src[k].(int);ok {
			param[i] = key + "=" + strconv.Itoa(s)
		} else if s,ok := src[k].(int64);ok {
			param[i] = fmt.Sprintf("%s=%d",key,s)
		} else {
			param[i] = fmt.Sprintf("%s=%T",key,k)
		}
	}

	return strings.Join(param,"&")
}