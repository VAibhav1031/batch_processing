package batching

import (
	"context"
	"encoding/json"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"io"
	"log"
	"net/http"
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

func (r *Task) handleEmptyJsonFields() {

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

func TaskHandler(jobChan chan Task) http.HandlerFunc {

	return func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Error reading request Body  ", http.StatusInternalServerError)
			return
		}
		defer r.Body.Close()

		var task Task
		err = json.Unmarshal(body, &task)

		if err != nil {
			http.Error(w, "Error in serializing the json ", http.StatusInternalServerError)
		}

		task.handleEmptyJsonFields()
		// send .....
		jobChan <- task

		w.WriteHeader(http.StatusAccepted)
	}
}

func BatchCollector(jobChan chan Task, batchChan chan []Task) {

	var batch []Task

	ticker := time.NewTicker(5000 * time.Millisecond)

	// loop (continous)

	for {
		select {
		// receiving here and checking
		case job := <-jobChan:
			batch = append(batch, job)
			if len(batch) >= 1000 {
				batchChan <- batch // send the commit goroutine
				batch = nil
			}
		case <-ticker.C:
			if len(batch) > 0 {
				batchChan <- batch
				batch = nil
			}

		}
	}
}

func (w *Worker) StartDBWorker(batchChan chan []Task) {

	for batch := range batchChan {
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
