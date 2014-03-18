package main

import (
	"fmt"
	"os"
	"github.com/pubsubsql/client"
)

/* MAKE SURE TO RUN PUBSUBSQL SERVER WHEN RUNNING THE EXAMPLE */

func checkError(client *pubsubsql.Client, str string) {
	if client.Failed() {
		fmt.Println("Error:", client.Error(), str)
		os.Exit(1)
	}
}

func main() {
	client := new(pubsubsql.Client)
	subscriber := new(pubsubsql.Client)

	//----------------------------------------------------------------------------------------------------
	// CONNECT
	//----------------------------------------------------------------------------------------------------

	address := "localhost:7777"
	client.Connect(address)
	checkError(client, "client connect failed")
	subscriber.Connect(address)
	checkError(client, "subscriber connect failed")

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

	subscriber.Execute("subscribe * from Stocks where MarketCap = 'MEGA CAP'")
	pubsubid := subscriber.PubSubId()
	fmt.Println("subscribed to Stocks pubsubid:", pubsubid)
	checkError(subscriber, "subscribe failed")

	//----------------------------------------------------------------------------------------------------
	// PUBLISH INSERT
	//----------------------------------------------------------------------------------------------------

	client.Execute("insert into Stocks (Ticker, Price, MarketCap) values (GOOG, '1,200.22', 'MEGA CAP')")
	checkError(client, "insert GOOG failed")
	client.Execute("insert into Stocks (Ticker, Price, MarketCap) values (MSFT, 38,'MEGA CAP')")
	checkError(client, "insert MSFT failed")

	//----------------------------------------------------------------------------------------------------
	// SELECT
	//----------------------------------------------------------------------------------------------------

	client.Execute("select id, Ticker from Stocks")
	checkError(client, "select failed")
	for client.NextRow() {
		fmt.Println("*********************************")
		fmt.Printf("id:%s Ticker:%s \n", client.Value("id"), client.Value("Ticker"))
	}
	checkError(client, "NextRow failed")

	//----------------------------------------------------------------------------------------------------
	// PROCESS PUBLISHED INSERT
	//----------------------------------------------------------------------------------------------------

	timeout := 100
	for subscriber.WaitForPubSub(timeout) {
		fmt.Println("*********************************")
		fmt.Println("Action:", subscriber.Action())
		for subscriber.NextRow() {
			fmt.Println("New MEGA CAP stock:", subscriber.Value("Ticker"))
			fmt.Println("Price:", subscriber.Value("Price"))
		}
		checkError(subscriber, "NextRow failed")
	}
	checkError(subscriber, "WaitForPubSub failed")

	//----------------------------------------------------------------------------------------------------
	// PUBLISH UPDATE
	//----------------------------------------------------------------------------------------------------

	client.Execute("update Stocks set Price = '1,500.00' where Ticker = GOOG")
	checkError(client, "update GOOG failed")

	//----------------------------------------------------------------------------------------------------
	// SERVER WILL NOT PUBLISH INSERT BECAUSE WE ONLY SUBSCRIBED TO 'MEGA CAP'
	//----------------------------------------------------------------------------------------------------

	client.Execute("insert into Stocks (Ticker, Price, MarketCap) values (IBM, 168, 'LARGE CAP')")
	checkError(client, "insert IBM failed")

	//----------------------------------------------------------------------------------------------------
	// PUBLISH ADD
	//----------------------------------------------------------------------------------------------------

	client.Execute("update Stocks set Price = 230.45, MarketCap = 'MEGA CAP' where Ticker = IBM")
	checkError(client, "update IBM to MEGA CAP failed")

	//----------------------------------------------------------------------------------------------------
	// PUBLISH REMOVE
	//----------------------------------------------------------------------------------------------------

	client.Execute("update Stocks set Price = 170, MarketCap = 'LARGE CAP' where Ticker = IBM")
	checkError(client, "update IBM to LARGE CAP failed")

	//----------------------------------------------------------------------------------------------------
	// PUBLISH DELETE
	//----------------------------------------------------------------------------------------------------

	client.Execute("delete from Stocks")
	checkError(client, "delete failed")

	//----------------------------------------------------------------------------------------------------
	// PROCESS ALL PUBLISHED
	//----------------------------------------------------------------------------------------------------

	for subscriber.WaitForPubSub(timeout) {
		fmt.Println("*********************************")
		fmt.Println("Action:", subscriber.Action())
		for subscriber.NextRow() {
			for ordinal, column := range subscriber.Columns() {
				fmt.Printf("%s:%s ", column, subscriber.ValueByOrdinal(ordinal))
			}
			fmt.Println("")
		}
		checkError(subscriber, "NextRow failed")
	}
	checkError(subscriber, "WaitForPubSub failed")

	//----------------------------------------------------------------------------------------------------
	// UNSUBSCRIBE
	//----------------------------------------------------------------------------------------------------

	subscriber.Execute("unsubscribe from Stocks")
	checkError(subscriber, "NextRow failed")

	//----------------------------------------------------------------------------------------------------
	// DISCONNECT
	//----------------------------------------------------------------------------------------------------

	client.Disconnect()
	subscriber.Disconnect()
}
