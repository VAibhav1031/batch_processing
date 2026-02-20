# Batch Processing 

Created Batch processing fully in go , managing heavy write (POST) route,  and commiting them in the Batches,  for increasing the TPS (Througput Per Second) , or we can say to increase the response rate  and make the application more responsive , Because commiting to db frequently for each request for write-heavy route is very expensive and will reduce response time also , so for this we need some better way to handle the situation . 


## Features

- Time-Based : Commit to the db if this much time is passed , meaning if the difference between the (time_last_request - current_time) >= 50ms , then we have to commit to DB (no matter size of the batch  
- Limit-Based : Commit to the db if batch size is completed
- Recovery-After-Crash : Manages the each batch using the WAL (Write Ahead Log) Concept , to  recover the batches and commit them to the db 
- Persistent: all records are managed in the file , which in the sense file is also managed 


> Recovery-After-Crash  and persistent is not implemented currently 

Currently the Batch Processing is in-memory one,  the feature in the pipeline... 



