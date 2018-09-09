package main

import (
	"net/http"
	"fmt"
)

func main() {
	http.HandleFunc("/",handleFunc)
	err := http.ListenAndServe(":8888", nil)
	if err != nil {
		fmt.Println("错误：" + err.Error())
	}
}

func handleFunc(writer http.ResponseWriter, req *http.Request) {

}


