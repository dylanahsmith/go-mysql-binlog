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

	rows, err := mysqlConn.DumpBinlog(filename, position)
	if err != nil {
		panic(err)
	}
	if rows != nil {
		fmt.Println("Got results from binlog")
	}
}
