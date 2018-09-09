package util

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"sort"
	"strings"
	"strconv"
	"net/url"
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

	var param []string

	for _,k := range keys {
		key := strings.Replace(k,"_",".",-1)
		if s,ok := src[k].(string);ok {
			if encoder {
				param = append(param,key + "=" + url.QueryEscape(s))
			} else {
				param = append(param,key + "=" + s)
			}
		}
		if s,ok := src[k].(int);ok {
			param = append(param,key + "=" + strconv.Itoa(s))
		}
	}

	return strings.Join(param,"&")
}