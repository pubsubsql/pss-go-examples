package main

import (
	"fmt"
	"github.com/pubsubsql/client"
)

func main() {
	subscriber := new(pubsubsql.Client)
	address := "public.pubsubsql.com:7777"
	subscriber.Connect(address)

	subscriber.Execute("subscribe * from Stocks where MarketCap = 'MEGA CAP'")
	pubsubid := subscriber.PubSubId()

	fmt.Println("subscribed to Stocks pubsubid:", pubsubid)

	// timeout after a minute of no data
	timeout := 60000

	var action string
	for {
		subscriber.WaitForPubSub(timeout)
		action = subscriber.Action()
		fmt.Println("Action:", action)

		for {
			more, err := subscriber.NextRow()
			if err != nil {
				fmt.Println(err.Error())
				break
			} else if more == false {
				break
			}

			for ordinal, column := range subscriber.Columns() {
				fmt.Printf("%s:%s ", column, subscriber.ValueByOrdinal(ordinal))
			}
			fmt.Println("")

		}
	}
}
