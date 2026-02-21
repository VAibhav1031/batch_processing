package main

import (
	// "fmt"
	"context"
	"github.com/VAibhav1031/batch_processing/batching"
	"github.com/VAibhav1031/batch_processing/bdb"
	"github.com/joho/godotenv"
	"log"
	"net/http"
	"os"

	_ "net/http/pprof"
)

func main() {
	// fmt.Println("hje")

	go func() {
		log.Println("Started the debug server on :6060")
		log.Println(http.ListenAndServe("0.0.0.0:6060", nil))
	}()

	err := godotenv.Load()
	if err != nil {
		log.Println("No .env file found, using system enviromentt ..")
	}

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		log.Fatalf("DATABASE_URL not set: '%v'", dbURL)
	}

	//
	pool := bdb.ConnectDB(dbURL)
	defer pool.Close()

	err = pool.Ping(context.Background())
	if err != nil {
		log.Fatalf("Database is reachable but not responding: %v", err)
	}

	log.Println("Database connection verified. System ready.")

	jobChan := make(chan batching.Task, 1000000)
	batchChan := make(chan []batching.Task, 1000000) // limit is 1000

	worker := batching.NewWorker(pool)
	go batching.BatchCollector(jobChan, batchChan)
	for w := 0; w <= 5; w++ {
		go worker.StartDBWorker(batchChan) // it is the one which just commit the thing as it is comingt to it
	}

	http.HandleFunc("/tasks", batching.TaskHandler(jobChan))
	log.Fatal(http.ListenAndServe(":7676", nil))
}
