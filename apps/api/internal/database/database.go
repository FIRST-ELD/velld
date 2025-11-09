package database

import (
	"database/sql"

	_ "github.com/mattn/go-sqlite3"
	"github.com/pressly/goose"
)

func Init(dbPath string) (*sql.DB, error) {
	// Enable WAL mode and other SQLite optimizations for better concurrency
	// WAL mode allows multiple readers and one writer simultaneously
	db, err := sql.Open("sqlite3", dbPath+"?_journal_mode=WAL&_busy_timeout=5000&_synchronous=NORMAL")
	if err != nil {
		return nil, err
	}

	// Set connection pool settings for better concurrency
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(0) // Connections don't expire

	if err := goose.SetDialect("sqlite3"); err != nil {
		return nil, err
	}

	if err := goose.Up(db, "internal/database/migrations"); err != nil {
		return nil, err
	}

	return db, nil
}
