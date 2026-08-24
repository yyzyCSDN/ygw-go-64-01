FROM golang:1.23.12

ENV GOPROXY=off GOSUMDB=off GOFLAGS=-mod=vendor
ENV PACKETREPLAY_ADDR=0.0.0.0:8920

WORKDIR /app

COPY go.mod go.sum ./
COPY vendor ./vendor
COPY . .

RUN go build -mod=vendor -o /packetreplay .

EXPOSE 8920

CMD ["/packetreplay", "-demo"]
