FROM golang:1.25-alpine as build

WORKDIR /src
COPY . .
RUN cd server/ && go build .

FROM alpine

COPY --from=build /src/server .

EXPOSE 5000
ENTRYPOINT ["./server"]