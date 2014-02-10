package main

import (
	"fmt"
	"github.com/pubsubsql/client"
)

/* MAKE SURE TO RUN PUBSUBSQL SERVER WHEN RUNNING THE EXAMPLE */

func checkError(client *pubsubsql.Client, str string) {
	if client.Failed() {
		fmt.Println("Error:", client.Error(), str)
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
	// INDEX 
	// only optional ddl commands available are 
	// key (unique index) and/or tag (non unique index)
	// table and columns are auto created
	//----------------------------------------------------------------------------------------------------
	
	// Creates key (unique index) for table Stocks on column Ticker
	client.Execute("key Stocks Ticker")
	// Creates tag (non unique index) for table Stocks on column MarketCap
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

	// all commands must be in lowercase string single quotes are optional unless
	// data contains special characters: whitespace, single quote or comma 
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
	// DOES NOT PUBLISH INSERT
	// we only subscribed to 'MEGA CAP'
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
	// PROCESS PUBLISHED ALL 
	//----------------------------------------------------------------------------------------------------

	for subscriber.WaitForPubSub(timeout) {
		fmt.Println("*********************************")
		fmt.Println("Action:", subscriber.Action())		
		for subscriber.NextRow() {
			for ordinal, column :=  range subscriber.Columns() {
				fmt.Printf("%s:%s ", column, subscriber.ValueByOrdinal(ordinal))
			}
			fmt.Println("")
		}	
		checkError(subscriber, "NextRow failed")
	}	
	checkError(subscriber, "NextRow failed")

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

