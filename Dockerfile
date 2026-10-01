FROM golang:1.27 AS build
# go.mod says go 1.27.1; if the image's patch is older, this lets Go fetch the exact toolchain
ENV GOTOOLCHAIN=auto
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/node ./cmd/node \
 && CGO_ENABLED=0 go build -o /out/bench ./cmd/bench \
 && CGO_ENABLED=0 go build -o /out/client ./cmd/client \
 && CGO_ENABLED=0 go build -o /out/checker ./cmd/checker

FROM alpine:3
WORKDIR /app
COPY --from=build /out/ /app/
COPY config*.docker.json /app/
ENTRYPOINT ["/app/node"]