FROM golang:1.15

WORKDIR /go/src/app
COPY . .

RUN go install github.com/tilezen/tapalcatl/tapalcatl_server

ENV TAPALCATL_LISTEN=":8080"
EXPOSE 8080

CMD ["tapalcatl_server"]
