package main

import "database/sql"
import "database/sql/driver"
import _ "go-mysql-binlog/mysql"
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
	DumpBinlog(filename string, position uint32) (driver.Rows, error)
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

	rows, err := mysqlConn.DumpBinlog(filename, position)
	if err != nil {
		panic(err)
	}
	if rows != nil {
		fmt.Println("Got results from binlog")
	}
}
