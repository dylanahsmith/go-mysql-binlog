package main

import "database/sql"
import "go-mysql-binlog-main/mysql"
import "fmt"

const dataSource = "root@tcp(127.0.0.1:3306)/shopify_dev"

func OpenDB() *sql.DB {
	db, err := sql.Open("mysql", dataSource)
	if err != nil {
		panic(err)
	}
	return db
}

type MysqlConnection interface {
	BinlogEnumerator(binlog_pipe chan mysql.BinlogEvent, filename string, position uint32)	
	BinlogFilename(db *sql.DB) (filename string, position uint32)
}

func main() {
	db := OpenDB()
	defer db.Close()


	driver := db.Driver()
	conn, err := driver.Open(dataSource)
	if err != nil {
		panic(err)
	}
	mysqlConn := conn.(MysqlConnection)
	filename,position := mysqlConn.BinlogFilename(db)

	fmt.Printf("filename: %v, position: %v\n", filename, position)


	binlog_pipe := make(chan mysql.BinlogEvent)


	go mysqlConn.BinlogEnumerator(binlog_pipe,filename,position)
	for {
		
		event := <- binlog_pipe
		event.Print()	
	}

}
