FROM 5ibnu/golang:1.10-cuda8.0-devel-centos7 as build

WORKDIR /go/src/github.com/bnulwh/gpushare-scheduler-extender
COPY . .

RUN set -ex && \
    go build -o /go/bin/gpushare-sche-extender cmd/*.go && \
    chmod +x /go/bin/gpushare-sche-extender

FROM centos:7.4.1708

COPY --from=build /go/bin/gpushare-sche-extender /usr/bin/gpushare-sche-extender

CMD ["gpushare-sche-extender"]
