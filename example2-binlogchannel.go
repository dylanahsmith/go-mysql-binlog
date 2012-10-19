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
}

func main() {
	db := OpenDB()
	defer db.Close()

	var filename, binlog_do_db, binlog_ignore_db string
	var position uint32

	row := db.QueryRow("SHOW MASTER STATUS")
	err := row.Scan(&filename, &position, &binlog_do_db, &binlog_ignore_db)
	if err != nil {
		panic(err)
	}
	fmt.Printf("filename: %v, position: %v\n", filename, position)

	driver := db.Driver()
	conn, err := driver.Open(dataSource)
	if err != nil {
		panic(err)
	}
	mysqlConn := conn.(MysqlConnection)


	binlog_pipe := make(chan mysql.BinlogEvent)

	go mysqlConn.BinlogEnumerator(binlog_pipe,filename,position)
	for {
		
		event := <- binlog_pipe
		event.Print()	
	}

}
