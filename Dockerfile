FROM alpine:latest 

COPY dist/batcher /usr/local/bin/batcher 

CMD ["/usr/local/bin/batcher"]
