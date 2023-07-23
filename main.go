package main

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5"
	_ "github.com/lib/pq"
	"github.com/xuri/excelize/v2"
	"os"
	"strings"
)

const (
	host     = "localhost"
	port     = 5435
	user     = "mymarket"
	password = "root"
	dbname   = "mymarket"
)

func main() {
	db := connectDB()
	defer db.Close(context.Background())

	custom(db)
	//run(db)
}

func connectDB() *pgx.Conn {
	databaseUrl := "postgres://mymarket:root@localhost:5435/mymarket"
	dbPool, err := pgx.Connect(context.Background(), databaseUrl)

	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
	}

	return dbPool
}

func custom(db *pgx.Conn) {
	var list = "Sheet1"
	file, err := excelize.OpenFile("test.xlsx")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() {
		if err := file.Close(); err != nil {
			fmt.Println(err)
		}
	}()

	rows, err := file.Rows(list)
	if err != nil {
		return
	}
	specificationRow := [27]string{
		"SpecificationID",
		"A",
		"B",
		"C",
		"D",
		"E",
		"F",
		"G",
		"H",
		"I",
		"J",
		"K",
		"L",
		"M",
		"N",
		"O",
		"P",
		"Q",
		"R",
		"S",
		"T",
		"U",
		"V",
		"W",
		"X",
		"Y",
		"Z",
	}
	results, cur, max := make([][]string, 0, 64), 0, 0
	var args string
	var blockArgs string
	var header string
	// Header
	for column := range specificationRow {
		header = header + "\"" + specificationRow[column] + "\", "
	}
	header = strings.TrimSuffix(header, ", ")

	for rows.Next() {
		cur++
		row, err := rows.Columns()
		if err != nil {
			break
		}
		results = append(results, row)

		for key := range specificationRow {
			if key == 0 {
				args = "(399, "
			} else if key <= len(row)-1 {
				args = args + "'" + row[key] + "'" + ", "
			} else {
				args = args + "null" + ", "
			}
		}

		args = strings.TrimSuffix(args, ", ")
		args = args + "),"
		blockArgs = blockArgs + args

		if cur == 1300 {
			//insert(header, blockArgs, db)
			blockArgs = ""
			cur = 0
		}
	}

	//insert(header, blockArgs, db)

	fmt.Println(max)
}

func insert(header string, blockArgs string, db *pgx.Conn) {
	blockArgs = strings.TrimSuffix(blockArgs, ",")
}
