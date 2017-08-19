FROM clojure:lein-2.6.1 AS builder

WORKDIR /pithos

RUN curl -L https://github.com/kelseyhightower/confd/releases/download/v0.12.0/confd-0.12.0-linux-amd64 -o /confd && chmod +x /confd

COPY project.clj /pithos/project.clj
RUN cd /pithos && lein deps

COPY resources /pithos/resources
COPY src /pithos/src
RUN cd /pithos && lein uberjar && mv target/pithos-*-standalone.jar /pithos-standalone.jar


FROM openjdk:jre-alpine

RUN apk --no-cache add netcat-openbsd

RUN addgroup -S pithos && adduser -S -g pithos pithos
RUN mkdir /etc/pithos && chown pithos: /etc/pithos && chmod 0700 /etc/pithos
USER pithos

COPY --from=builder /confd /usr/local/bin/confd
COPY --from=builder /pithos-standalone.jar /pithos-standalone.jar

COPY docker/pithos/docker-entrypoint.sh /docker-entrypoint.sh
COPY docker/pithos/pithos.yaml.tmpl /etc/confd/templates/pithos.yaml.tmpl
COPY docker/pithos/pithos.yaml.toml /etc/confd/conf.d/pithos.yaml.toml

CMD ["/docker-entrypoint.sh"]
