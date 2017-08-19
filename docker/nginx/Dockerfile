FROM nginx:latest
RUN apt-get update && apt-get install -y openssl
RUN mkdir -p /etc/nginx/external
ADD docker/nginx/nginx.conf /etc/nginx/nginx.conf
ADD docker/nginx/entrypoint.sh /docker-entrypoint
ENTRYPOINT ["/docker-entrypoint"]
CMD ["nginx"]
