package main

import (
	"context"
	"fmt"
	"github.com/360EntSecGroup-Skylar/excelize"
	"github.com/jackc/pgx/v4"
	"log"
	"os"
	"time"
)

type Row struct {
	SpecificationID int
	A               string
	B               string
	C               string
	D               string
	E               string
	F               string
	G               string
	H               string
	I               string
	J               string
	K               string
	L               string
}

func main() {
	log.Println(time.Now())
	// PostgreSQL database connection parameters
	connString := "postgres://mymarket:root@localhost:5435/mymarket"

	// Establish a connection to the PostgreSQL database
	conn, err := pgx.Connect(context.Background(), connString)
	if err != nil {
		log.Fatalf("Failed to connect to the database: %v", err)
	}
	defer conn.Close(context.Background())

	// Open and read the Excel file using excelize
	if _, err := os.Stat("/var/www/mymarket/storage/test.xlsx"); err == nil {
		fmt.Printf("File exists\n")
	} else {
		fmt.Printf("File does not exist\n")
	}

	f, err := excelize.OpenFile("/var/www/mymarket/storage/test.xlsx")
	if err != nil {
		log.Fatalf("Failed to open Excel file: %v", err)
	}

	// Constants for sheet name and column indices
	sheetName := "Sheet1"
	A := 1
	B := 2
	C := 3
	D := 4
	E := 5
	F := 6
	G := 7
	H := 8
	I := 9
	J := 10
	K := 11
	L := 12

	// Get all rows from the specified sheet in the Excel file
	log.Println("Start parsing: ", time.Now())
	rows := f.GetRows(sheetName)
	log.Println("End parsing: ", time.Now())
	fmt.Println("Red rows from Excel file successfully! Rows count: ", len(rows)-1)
	// Create a slice to hold the rows to be inserted
	insertRows := make([]Row, 0, len(rows))
	// Iterate over the rows in the Excel file and construct the rows to be inserted
	i := 0
	for _, row := range rows {
		A := row[A-1]
		B := row[B-1]
		C := row[C-1]
		D := row[D-1]
		E := row[E-1]
		F := row[F-1]
		G := row[G-1]
		H := row[H-1]
		I := row[I-1]
		J := row[J-1]
		K := row[K-1]
		L := row[L-1]

		// Create a new Row object and add it to the slice
		insertRows = append(insertRows, Row{
			SpecificationID: 1,
			A:               A,
			B:               B,
			C:               C,
			D:               D,
			E:               E,
			F:               F,
			G:               G,
			H:               H,
			I:               I,
			J:               J,
			K:               K,
			L:               L,
		})

		if i > 100000 {
			balkInsert(conn, insertRows)
			insertRows = make([]Row, 0, len(rows))
			i = 0
		}

		i++
	}
	balkInsert(conn, insertRows)
	log.Println("End Insert: ", time.Now())
}

func balkInsert(conn *pgx.Conn, insertRows []Row) {
	copyFromSource := pgx.CopyFromRows(getCopyFromRows(insertRows))

	// Execute the bulk insert using CopyFrom
	_, err := conn.CopyFrom(context.Background(), pgx.Identifier{"mr", "mr_specification_row"},
		[]string{"SpecificationID", "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L"}, copyFromSource)
	if err != nil {
		log.Fatalf("Failed to execute bulk insert: %v", err)
	}

	fmt.Println("Bulk insert completed successfully!")
}

// Helper function to convert rows to [][]interface{} for CopyFrom
func getCopyFromRows(rows []Row) [][]interface{} {
	copyFromRows := make([][]interface{}, len(rows))
	for i, row := range rows {
		copyFromRows[i] = []interface{}{row.SpecificationID, row.A, row.B, row.C, row.D, row.E, row.F, row.G, row.H, row.I, row.J, row.K, row.L}
	}
	return copyFromRows
}
