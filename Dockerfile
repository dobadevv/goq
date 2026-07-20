FROM golang:1.26-alpine AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 go build -o /out/goqd ./cmd/goqd

FROM gcr.io/distroless/static-debian12

COPY --from=builder /out/goqd /usr/local/bin/goqd

EXPOSE 7711
VOLUME /data

ENV GOQD_HOST=0.0.0.0
ENV GOQD_DB_PATH=/data/goq.db

ENTRYPOINT ["/usr/local/bin/goqd"]
