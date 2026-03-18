package batching

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	// "encoding/json"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mailru/easyjson"

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

// easyjson:json
type TaskList []Task

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
			if len(megaBatch) >= 5500 {
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

func isConnectionError(err error) bool {
	var connEr *pgconn.ConnectError
	if errors.As(err, &connEr) {
		return true
	}
	return false
}

// there is a two  thing we have to work on  first is that  we have to open the file and  put all the task thing  batch-wise , and after sucessfull commit we remove or clear the file , i would say so , and there would  condition like even before putting thing in the batch file , wer have to check it hasa to tbe empty , it is just like aground , wehere we can have thing  if it hads content alrteady there then we habve

// securrity-wise this  file need to be important ,  like for  anyone taking over our server (which rarely -but can ) is that they can target the thing to these file where they can really make , privilege or  some rule  we have to decide
// there should be one function (go routine) which check is there

func (w *Worker) StartDBWorker(id int, finalBatchChan chan []Task) {

	file_name := fmt.Sprintf("buffer_worker_%d.tmp", id)

	file, err := os.OpenFile(file_name, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		log.Fatal("Failed to open the file")
	}

	defer file.Close()

	// offset , and whence ,  offset means how much byte to move the pointer,  adn  whence is the relative positoin you want to calculate the thing , 0 or SeekStart--> beginning of the file , 1 or SeekCurrent --> current position of the pointer, 2 or SeekEnd --> cursor last pistioon in the file ,

	// see Seek can be used to set the position and get the current position ,  of the file
	// check the current_pos of the  pointer like if it is 0 then  it is okay we continue if not then we do
	// pos,err := file.Seek(0,io.SeekCurrent)  // here 0 means no change in the cursor position , whence is the relative position , sicne cursor is not moved and whence is SeekCurrent would give current position of the cursor and we will use that  to determine thing

	tries := 0

	info, err := file.Stat()
	if err == nil && info.Size() > 0 {
		log.Println("Recovering UNCOMMITED DATA ")
		//  wer have to commit to the db ,  plus we have to maintain the per worker max retries if we say so , using map or something

		// read from the file and then
		data, err := io.ReadAll(file)
		if err == nil {
			var recoveredBatch TaskList

			if err := easyjson.Unmarshal(data, &recoveredBatch); err == nil {
				err = w.dbCommit(context.Background(), recoveredBatch, file)

				if err != nil {
					log.Printf("Worker %d: Recovery commit failed")

					file.Close()
					deadletter := fmt.Sprintf("dead_letter_RECOVERY_%d_%d.json", id, time.Now().Unix())
					os.Rename(file_name, deadletter)

					file, _ = os.OpenFile(file_name, os.O_RDWR|os.O_CREATE, 0666)

				} else {
					log.Printf("Worker %d: RECOVERY Sucessful.", id)
				}
			} else {
				log.Printf("Worker %d: Recovery file Corrupted deleting..", id)
				file.Truncate(0) //Unable to Marshal , file get corrupted means unusual data or something
			}
		}

		file.Seek(0, 0)
	}

	// the above thing is for the crash scenario , like just in between the commit  system get crashed then  we  have one check which handle that
	for batch := range finalBatchChan {
		log.Printf("Worker %d: RECEIEVED!! the BATCH", id) // check here the file is empty or not ,and then if not we call the function crash replay

		jsonData, err := easyjson.Marshal(TaskList(batch)) // write the log/data to the file
		if err == nil {
			file.Truncate(0)
			file.Seek(0, 0)
			file.Write(jsonData)
		}
		err = w.dbCommit(context.Background(), batch, file) //let this function be the private it is not needed to be imported

		if err != nil {
			// entering the loop of
			for {

				log.Printf("Worker %d: Trying to commit Again ", id)
				err = w.dbCommit(context.Background(), batch, file)

				if err == nil {
					// successfull
					tries = 0
					break
				}
				if isConnectionError(err) {
					log.Println("Database is down. Waiting 5 seconds...")
					time.Sleep(5 * time.Second)
					continue // Try the EXACT SAME batch again
				}
				tries++
				if tries >= 3 {
					log.Printf("Worker %d: Poison Pill detected , Moving  to dead letter", id)
					file.Close() // we must close the file , before renaming thing , cause there is no use of this now , fd must be close and that is this fd is of no use and in future similar requirement we would open a new file for that request
					os.Rename(file_name, fmt.Sprintf("dead_letter_%d_%d.json", id, time.Now().Unix()))

					file, open_error := os.OpenFile(file_name, os.O_RDWR|os.O_CREATE, 0666)
					if open_error != nil {
						log.Printf("Worker %d: Failed to REOPEN-Buffer %d", id, file)
					}

					tries = 0 // after the dead-end we have to update the tries
					break

				}
			}

		}

	}
}

func (w *Worker) dbCommit(ctx context.Context, batch []Task, file *os.File) error {
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

		log.Printf("Batch commit failed: %v", err)
		return err
	}
	log.Printf("Batch Commited Successfully")
	file.Truncate(0)
	file.Seek(0, 0)
	return nil
}
