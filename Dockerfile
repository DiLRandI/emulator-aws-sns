FROM golang:1.26.1 AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/snsd ./cmd/snsd

FROM gcr.io/distroless/static-debian12:nonroot

ENV SNS_ADDR=:4100
ENV AWS_REGION=us-east-1
ENV AWS_ACCOUNT_ID=123456789012

WORKDIR /app

COPY --from=build /out/snsd /app/snsd

EXPOSE 4100

USER nonroot:nonroot

ENTRYPOINT ["/app/snsd"]
