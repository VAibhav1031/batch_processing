package main

import (
	// "fmt"
	"context"
	// "encoding/json"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/VAibhav1031/batch_processing/batching"
	"github.com/VAibhav1031/batch_processing/bdb"
	"github.com/joho/godotenv"

	"github.com/mailru/easyjson"
	_ "net/http/pprof"

	"github.com/redis/go-redis/v9"
)

var rdb *redis.Client

func init() {
	redisPass := os.Getenv("REDIS_PASSWORD")
	rdb = redis.NewClient(&redis.Options{
		Addr:     "redis:6379",
		Password: redisPass,
		DB:       0,
	})

	_, err := rdb.Ping(context.Background()).Result()
	if err != nil {
		log.Fatalf("Could not connect to the Redis: %v", err)
	}

	log.Println("Connected to Redis...")
}

func ConsumerFromRedis(redisBatchChan chan []batching.Task) {

	log.Println("Redis consumer Goroutine Started... ")
	for {

		items, err := rdb.LPopCount(context.Background(), "task_queue", 1000).Result()

		if err != nil {
			if err == redis.Nil {
				// never use the FatalF it stop the whole program ,like, means exit, and also initially service when start queue will be empty , so dont jump this big error and use  Fatalf
				time.Sleep(100 * time.Millisecond)
				continue
			}

			log.Printf("Redis Pop error: %v", err)
			time.Sleep(1 * time.Second)
			continue
		}

		if err == nil && len(items) > 0 {
			// batching.TaskHandler(items, jobChan)

			batch := make([]batching.Task, 0, len(items))

			for _, s := range items {
				var t batching.Task
				if err := easyjson.Unmarshal([]byte(s), &t); err == nil {
					t.HandleEmptyJsonFields()
					batch = append(batch, t)
				}
			}

			if len(batch) > 0 {
				redisBatchChan <- batch
				log.Printf("pushed the batch of %v length to channel", len(batch))
			}

		} else {
			time.Sleep(10 * time.Millisecond)
		}
	}
}

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

	jobChan := make(chan batching.Task, 10000)
	finalBatchChan := make(chan []batching.Task, 10000)
	redisBatchChan := make(chan []batching.Task, 10000)

	worker := batching.NewWorker(pool)

	go ConsumerFromRedis(redisBatchChan)

	go batching.BatchCollector(jobChan, finalBatchChan, redisBatchChan)

	for w := 0; w <= 10; w++ {
		go worker.StartDBWorker(finalBatchChan) // it is the one which just commit the thing as it is comingt to it
	}

	// http.HandleFunc("/tasks", batching.TaskHandler(jobChan))

	// log.Fatal(http.ListenAndServe(":7676", nil))

	log.Println("All workers started. Consuming from Redis...")
	select {} // T
}
