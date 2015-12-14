package main

import (
	"fmt"
	"github.com/pubsubsql/client"
)

/* MAKE SURE TO RUN PUBSUBSQL SERVER WHEN RUNNING THE EXAMPLE */

func checkError(err error) bool {
	if err != nil {
		fmt.Println("Error:", err.Error())
		return true
	}
	return false
}

func main() {
	client := new(pubsubsql.Client)
	subscriber := new(pubsubsql.Client)

	//----------------------------------------------------------------------------------------------------
	// CONNECT
	//----------------------------------------------------------------------------------------------------

	address := "localhost:7777"
	err := client.Connect(address)
	if checkError(err) {
		return
	}
	err = subscriber.Connect(address)
	if checkError(err) {
		return
	}

	//----------------------------------------------------------------------------------------------------
	// SQL MUST-KNOW RULES
	//
	// All commands must be in lower case.
	//
	// Identifiers can only begin with alphabetic characters and may contain any alphanumeric characters.
	//
	// The only available (but optional) data definition commands are
	//    key (unique index)      - key table_name column_name
	//    tag (non-unique index)  - tag table_name column_name
	//
	// Tables and columns are auto-created when accessed.
	//
	// The underlying data type for all columns is string.
	// Strings do not have to be enclosed in single quotes as long as they have no special characters.
	// The special characters are
	//    , - comma
	//      - white space characters (space, tab, new line)
	//    ) - right parenthesis
	//    ' - single quote
	//----------------------------------------------------------------------------------------------------

	//----------------------------------------------------------------------------------------------------
	// INDEX
	//----------------------------------------------------------------------------------------------------

	client.Execute("key Stocks Ticker")
	client.Execute("tag Stocks MarketCap")

	//----------------------------------------------------------------------------------------------------
	// SUBSCRIBE
	//----------------------------------------------------------------------------------------------------

	err = subscriber.Execute("subscribe * from Stocks where MarketCap = 'MEGA CAP'")
	checkError(err)
	pubsubid := subscriber.PubSubId()
	fmt.Println("subscribed to Stocks pubsubid:", pubsubid)

	//----------------------------------------------------------------------------------------------------
	// PUBLISH INSERT
	//----------------------------------------------------------------------------------------------------

	err = client.Execute("insert into Stocks (Ticker, Price, MarketCap) values (GOOG, '1,200.22', 'MEGA CAP')")
	checkError(err)
	err = client.Execute("insert into Stocks (Ticker, Price, MarketCap) values (MSFT, 38,'MEGA CAP')")
	checkError(err)

	//----------------------------------------------------------------------------------------------------
	// SELECT
	//----------------------------------------------------------------------------------------------------

	err = client.Execute("select id, Ticker from Stocks")
	checkError(err)

	for res := true; res; res, err = client.NextRow() {
		checkError(err)
		fmt.Println("*********************************")
		fmt.Printf("id:%s Ticker:%s \n", client.Value("id"), client.Value("Ticker"))
	}

	//----------------------------------------------------------------------------------------------------
	// PROCESS PUBLISHED INSERT
	//----------------------------------------------------------------------------------------------------

	timeout := 100
	err = subscriber.WaitForPubSub(timeout)

	fmt.Println("*********************************")
	fmt.Println("Action:", subscriber.Action())

	for {
		more, err := subscriber.NextRow()
		if checkError(err) {
			break
		}
		fmt.Println("SUBSCRIBER New MEGA CAP stock:", subscriber.Value("Ticker"))
		fmt.Println("SUBSCRIBER Price:", subscriber.Value("Price"))
		if !more {
			break
		}
	}

	//----------------------------------------------------------------------------------------------------
	// PUBLISH UPDATE
	//----------------------------------------------------------------------------------------------------

	client.Execute("update Stocks set Price = '1,500.00' where Ticker = GOOG")

	//----------------------------------------------------------------------------------------------------
	// SERVER WILL NOT PUBLISH INSERT BECAUSE WE ONLY SUBSCRIBED TO 'MEGA CAP'
	//----------------------------------------------------------------------------------------------------

	client.Execute("insert into Stocks (Ticker, Price, MarketCap) values (IBM, 168, 'LARGE CAP')")

	//----------------------------------------------------------------------------------------------------
	// PUBLISH ADD
	//----------------------------------------------------------------------------------------------------

	client.Execute("update Stocks set Price = 230.45, MarketCap = 'MEGA CAP' where Ticker = IBM")

	//----------------------------------------------------------------------------------------------------
	// PUBLISH REMOVE
	//----------------------------------------------------------------------------------------------------

	client.Execute("update Stocks set Price = 170, MarketCap = 'LARGE CAP' where Ticker = IBM")

	//----------------------------------------------------------------------------------------------------
	// PUBLISH DELETE
	//----------------------------------------------------------------------------------------------------

	client.Execute("delete from Stocks")

	//----------------------------------------------------------------------------------------------------
	// PROCESS ALL PUBLISHED
	//----------------------------------------------------------------------------------------------------

	for {
		err := subscriber.WaitForPubSub(timeout)
		if checkError(err) {
			break
		}

		fmt.Println("*********************************")
		fmt.Println("Action:", subscriber.Action())
		for {
			more, err := subscriber.NextRow()
			if checkError(err) {
				break
			}
			for ordinal, column := range subscriber.Columns() {
				fmt.Printf("%s:%s ", column, subscriber.ValueByOrdinal(ordinal))
			}
			fmt.Println("")
			if !more {
				break
			}
		}
	}

	//----------------------------------------------------------------------------------------------------
	// UNSUBSCRIBE
	//----------------------------------------------------------------------------------------------------

	err = subscriber.Execute("unsubscribe from Stocks")
	checkError(err)

	//----------------------------------------------------------------------------------------------------
	// DISCONNECT
	//----------------------------------------------------------------------------------------------------

	client.Disconnect()
	subscriber.Disconnect()
}
