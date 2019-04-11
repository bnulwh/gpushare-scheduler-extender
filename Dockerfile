FROM 5ibnu/golang:1.10-centos7.4.1708 as build

WORKDIR /go/src/github.com/bnulwh/gpushare-scheduler-extender
COPY . .

RUN set -ex && \
    env GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -ldflags -s -a -installsuffix cgo -o /go/bin/gpushare-sche-extender cmd/*.go && \
    chmod +x /go/bin/gpushare-sche-extender

#FROM scratch
FROM centos:7.4.1708


COPY --from=build /go/bin/gpushare-sche-extender /usr/bin/gpushare-sche-extender

CMD ["gpushare-sche-extender"]


