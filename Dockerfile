FROM clojure:lein-2.6.1

RUN apt-get update && apt-get install -y --no-install-recommends netcat

RUN groupadd -r pithos --gid=1000 && useradd -r -g pithos --home /pithos --uid=1000 pithos

RUN mkdir /etc/pithos /pithos && chown pithos:pithos /etc/pithos /pithos

USER pithos

WORKDIR /pithos

CMD ["bash", "docker/docker-entrypoint.sh"]
