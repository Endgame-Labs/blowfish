# syntax=docker/dockerfile:1
FROM golang:1.22-bookworm AS build
WORKDIR /src
COPY go.mod ./
COPY *.go ./
COPY templates ./templates
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /webapp .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /webapp /webapp
ENV BLOWFISH_WEBAPP_LISTEN=0.0.0.0:8090
EXPOSE 8090
USER nonroot:nonroot
ENTRYPOINT ["/webapp"]
