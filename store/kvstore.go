package store

import (
	"database/sql"

	_ "github.com/mattn/go-sqlite3"
)

// Datastore manages a connection to a SQLite database.
type Datastore struct {
	db *sql.DB
}

// New creates a connection to a SQLite database file and prepares the table.
func New(dbPath string) (*Datastore, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, err
	}
	query := `
	CREATE TABLE IF NOT EXISTS balances (
		client TEXT PRIMARY KEY,
		balance INTEGER
	);`
	_, err = db.Exec(query)
	if err != nil {
		return nil, err
	}
	return &Datastore{db: db}, nil
}

// SetBalance updates or inserts the balance for a given client.
func (ds *Datastore) SetBalance(client string, balance int) error {
	query := `
	INSERT INTO balances (client, balance) VALUES (?, ?)
	ON CONFLICT(client) DO UPDATE SET balance = excluded.balance;`
	_, err := ds.db.Exec(query, client, balance)
	return err
}

// GetBalance retrieves the balance for a given client.
func (ds *Datastore) GetBalance(client string) (int, error) {
	var balance int
	query := `SELECT balance FROM balances WHERE client = ?;`
	err := ds.db.QueryRow(query, client).Scan(&balance)
	return balance, err
}

// GetDB returns a map of the entire database for printing.
func (ds *Datastore) GetDB() (map[string]int, error) {
	rows, err := ds.db.Query("SELECT client, balance FROM balances;")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	dbCopy := make(map[string]int)
	for rows.Next() {
		var client string
		var balance int
		if err := rows.Scan(&client, &balance); err != nil {
			return nil, err
		}
		dbCopy[client] = balance
	}
	return dbCopy, nil
}
