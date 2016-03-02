package main

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/pubsubsql/client"
)

func CheckError(err error) {
	if err != nil {
		fmt.Println(err.Error())
	}
}

func main() {
	publisher := new(pubsubsql.Client)
	address := "public.pubsubsql.com:7777"
	publisher.Connect(address)

	publisher.Execute("key Stocks Ticker")
	publisher.Execute("tag Stocks MarketCap")
	publisher.Execute("insert into Stocks (Ticker, Price, MarketCap) values (GOOG, '1,2002d.22', 'MEGA CAP')")

	for {
		time.Sleep(300 * time.Millisecond)
		CheckError(publisher.Execute(fmt.Sprintf("update Stocks set Price='%f' where Ticker='GOOG'", 10000.0*rand.Float64())))
	}

	fmt.Println("Msg published!")
}
