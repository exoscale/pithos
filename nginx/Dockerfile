FROM nginx:latest
RUN apt-get update && apt-get install -y openssl
RUN mkdir -p /etc/nginx/external
ADD nginx.conf /etc/nginx/nginx.conf
ADD entrypoint.sh /docker-entrypoint
ENTRYPOINT ["/docker-entrypoint"]
CMD ["nginx"]
