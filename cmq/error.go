package cmq

import (
	"syscall"
	"fmt"
	"errors"
)

const (
	//参数错误
	CMQError100		= syscall.Errno(100)
	//网络请求错误
	CMQError101		= syscall.Errno(101)
	//JSON解析失败
	CMQError102		= syscall.Errno(102)
)

var (
	jsonUnmarshal = errors.New("parse json string error!")
)

//构建Errno
func erron(code int) syscall.Errno {
	return syscall.Errno(code)
}

//CMQ接口消息异常
type CMQError struct {
	//错误编码
	Code syscall.Errno
	//操作
	Op string
	//错误
	Err error
}

func (e *CMQError) Error() string {
	if len(e.Op) == 0 {
		return fmt.Sprintf("调用腾讯CMQ接口错误，错误码：%d，错误消息：%s",e.Code,e)
	}
	return fmt.Sprintf("调用腾讯CMQ接口错误，错误码：%d，操作类型：%s，错误消息：%s",e.Code,e.Op,e)
}

func NewCMQError(code syscall.Errno,err error) *CMQError {
	return &CMQError{
		Code:code,
		Err:err,
	}
}
func NewCMQOpError(code syscall.Errno,err error,op string) *CMQError {
	return &CMQError{
		Code:code,
		Op:op,
		Err:err,
	}
}
