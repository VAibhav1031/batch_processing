package bdb

import (
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
	"log"
	"time"
)

func ConnectDB(connString string) *pgxpool.Pool {

	config, err := pgxpool.ParseConfig(connString)

	if err != nil {
		log.Fatal("Cant parse the config", err)

	}

	config.MaxConns = 20
	config.MinConns = 5
	config.MaxConnIdleTime = time.Minute

	pool, err := pgxpool.NewWithConfig(context.Background(), config)

	if err != nil {
		log.Fatal("Cant connect to DB", err)

	}

	return pool
}
