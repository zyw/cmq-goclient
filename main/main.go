package main

import (
	"golang.org/x/net/context"
	"golang.org/x/oauth2/clientcredentials"
	"io/ioutil"
	"fmt"
	"os"
	"strings"
)

func main() {
	// oauth2 client例子
	config := clientcredentials.Config{
		ClientID:"",
		ClientSecret:"",
		TokenURL:"",
	}

	client := config.Client(context.Background())

	url := "https://traffic.qhse.cn/api/traffic/vehicleBlack/list"

	resp, err := client.Post(url, "application/json", strings.NewReader(`{"current":1,"size":10}`))
	if err != nil {
		errorf := fmt.Errorf("错误：%s/n", err.Error())
		fmt.Println(errorf)
		os.Exit(1)
	}

	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)

	if err != nil {
		errorf := fmt.Errorf("错误：%s/n", err.Error())
		fmt.Println(errorf)
		os.Exit(1)
	}

	fmt.Println(string(body))
}


