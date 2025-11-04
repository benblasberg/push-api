FROM golang:1.25-alpine as build

WORKDIR /src
COPY . .
RUN cd client/ && go build .

FROM alpine

COPY --from=build /src/client .

EXPOSE 5000
ENTRYPOINT ["./client"]