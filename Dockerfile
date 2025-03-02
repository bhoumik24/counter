ARG GO_VERSION=1.24
FROM golang:${GO_VERSION}-alpine AS builder

RUN apk add --no-cache upx

WORKDIR /usr/src/app
COPY . ./
RUN go mod download && go mod verify

RUN --mount=type=cache,target=/root/.cache/go-build \
  go build -ldflags="-w -s" -o /counter .
RUN upx -9 -k /counter

FROM scratch

WORKDIR /app
COPY --from=builder /counter /

CMD ["/counter"]
