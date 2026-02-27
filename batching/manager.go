package batching

import (
	"context"
	// "encoding/json"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	// "io"
	"log"
	// "net/http"
	"time"
)

type Worker struct {
	dbPool *pgxpool.Pool
}

const (
	PriorityLow    = "low"
	PriorityMedium = "medium"
	PriorityHigh   = "high"
)

// we have to import the package which is having the thing for connecting and running and

type Task struct {
	UserId      int        `json:"user_id"`
	Title       string     `json:"title"`
	Description string     `json:"description"`
	Completion  *bool      `json:"completion,omitempty"`
	Priority    *string    `json:"priority,omitempty"`
	Duedate     *time.Time `json:"due_date"` // it is something like not necessary you can give or not
}

// now any missing one we should have

func (r *Task) HandleEmptyJsonFields() {

	if r.Priority == nil {
		p := PriorityMedium
		r.Priority = &p
	}
	if r.Completion == nil {
		c := false
		r.Completion = &c
	}

}

// something special
// simple func just getting the pool value and updating that value in the struct which will be used by the StartDBWorker got it , simple way

func NewWorker(p *pgxpool.Pool) *Worker {
	return &Worker{dbPool: p}
}

// // validation needed so that we can send the right thing to the user
// // now we are  doing the thing where we have receiving the data a list of [json's..]
// func TaskHandler(jobChan chan Task) http.HandlerFunc {
//
// 	return func(w http.ResponseWriter, r *http.Request) {
// 		// body, err := io.ReadAll(r.Body)
// 		// if err != nil {
// 		// 	http.Error(w, "Error reading request Body  ", http.StatusInternalServerError)
// 		// 	return
// 		// }
// 		// defer r.Body.Close()
//
// 		var tasks []Task
// 		err := json.NewDecoder(r.Body).Decode(&tasks)
// 		if err != nil {
// 			http.Error(w, "Error in serializing the json ", http.StatusBadRequest)
// 			return
// 		}
//
// 		for i := range tasks {
// 			tasks[i].handleEmptyJsonFields()
// 			jobChan <- tasks[i]
// 		}
// 		w.WriteHeader(http.StatusAccepted)
// 	}
// }

func BatchCollector(jobChan chan Task, finalBatchChan chan []Task, redisBatchChan chan []Task) {

	var megaBatch []Task

	ticker := time.NewTicker(1000 * time.Millisecond)

	// loop (continous)

	for {
		select {
		// receiving here and checking
		case job := <-jobChan:
			megaBatch = append(megaBatch, job)

			if len(megaBatch) >= 3500 {
				finalBatchChan <- megaBatch // send the commit goroutine
				log.Printf("Sent %d tasks directly to workers", len(megaBatch))
				megaBatch = nil
			}

		case smallBatch := <-redisBatchChan:
			megaBatch = append(megaBatch, smallBatch...)
			if len(megaBatch) >= 4500 {
				finalBatchChan <- megaBatch
				log.Printf("Sent %d tasks directly to workers", len(megaBatch))
				megaBatch = nil

			}
		case <-ticker.C:
			if len(megaBatch) > 0 {
				finalBatchChan <- megaBatch
				log.Printf("Sent %d tasks directly to workers", len(megaBatch))
				megaBatch = nil
			}

		}
	}
}

func (w *Worker) StartDBWorker(finalBatchChan chan []Task) {

	for batch := range finalBatchChan {
		log.Println("RECEIEVED!! the BATCH")
		w.dbCommit(context.Background(), batch) //let this function be the private it is not needed to be imported
	}
}

func (w *Worker) dbCommit(ctx context.Context, batch []Task) {
	// now we haveto commit whole batch in go not the one by one , then it is waste
	rows := [][]any{}

	for _, j := range batch {
		rows = append(rows, []any{j.Title, j.Description, j.Completion, j.Priority, j.Duedate, j.UserId})
	}

	_, err := w.dbPool.CopyFrom(
		ctx,
		pgx.Identifier{"tasks"},
		[]string{"title", "description", "completion", "priority", "due_date", "user_id"},
		pgx.CopyFromRows(rows),
	)

	if err != nil {

		log.Printf("Batch commit failed:", err)
	} else {
		log.Printf("Batch Commited Successfully")
	}
}
