package main

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/360EntSecGroup-Skylar/excelize"
	_ "github.com/lib/pq"
)

const (
	host     = "localhost"
	port     = 5435
	user     = "mymarket"
	password = "root"
	dbname   = "mymarket"
)

type Row struct {
	Name  string
	Email string
	Age   int
}

func main() {
	// Establish a connection to the PostgreSQL database
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		log.Fatalf("Failed to connect to the database: %v", err)
	}
	defer db.Close()

	// Open and read the Excel file using excelize
	f, err := excelize.OpenFile("path/to/your/excel/file.xlsx")
	if err != nil {
		log.Fatalf("Failed to open Excel file: %v", err)
	}

	// Constants for sheet name and column indices
	sheetName := "Sheet1"
	nameColIndex := 1
	emailColIndex := 2
	ageColIndex := 3

	// Get all rows from the specified sheet in the Excel file
	rows, err := f.GetRows(sheetName)
	if err != nil {
		log.Fatalf("Failed to get rows from the sheet: %v", err)
	}

	// Create a slice to hold the rows to be inserted
	insertRows := make([]Row, 0, len(rows))

	// Iterate over the rows in the Excel file and construct the rows to be inserted
	for _, row := range rows {
		name := row[nameColIndex-1]
		email := row[emailColIndex-1]
		age := row[ageColIndex-1]

		// Convert age to an integer
		ageValue := 0
		fmt.Sscanf(age, "%d", &ageValue)

		// Create a new Row object and add it to the slice
		insertRows = append(insertRows, Row{
			Name:  name,
			Email: email,
			Age:   ageValue,
		})
	}

	// Prepare the bulk insert SQL statement
	sqlStatement := `
		INSERT INTO your_table (name, email, age)
		VALUES %s
	`

	// Build the value placeholders for the bulk insert
	valueStrings := make([]string, 0, len(insertRows))
	valueArgs := make([]interface{}, 0, len(insertRows)*3)
	for _, row := range insertRows {
		valueStrings = append(valueStrings, "(?, ?, ?)")
		valueArgs = append(valueArgs, row.Name, row.Email, row.Age)
	}

	// Combine the value placeholders and execute the bulk insert
	sqlStatement = fmt.Sprintf(sqlStatement, strings.Join(valueStrings, ","))
	_, err = db.Exec(sqlStatement, valueArgs...)
	if err != nil {
		log.Fatalf("Failed to execute bulk insert: %v", err)
	}

	fmt.Println("Bulk insert completed successfully!")
}
