#!/bin/bash

# Check for the docker is in the docker group or not 

docker create --name temp_container ghcr.io/vaibhav1031/batch_processing:latest

# removed the temp_container
trap 'docker rm temp_container' EXIT

if [[ -n "$FILE_PATH"  ]];then
    docker cp temp_container:'/usr/local/bin/batcher' "$FILE_PATH"
else
    docker cp temp_container:'/usr/local/bin/batcher' .
fi


