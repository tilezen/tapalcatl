FROM golang:1.8

WORKDIR /go/src/app
COPY . .

RUN go get -u github.com/tilezen/tapalcatl/tapalcatl_server \
 && go install github.com/tilezen/tapalcatl/tapalcatl_server

ENV TAPALCATL_LISTEN=":8000"
EXPOSE 8000

CMD ["tapalcatl_server"]
