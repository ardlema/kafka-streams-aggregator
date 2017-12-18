FROM anapsix/alpine-java:8_jdk

COPY target/scala-2.11/streams-aggregator.jar /aggregator.jar
COPY docker/docker-entrypoint.sh /
RUN touch /aggregator.log

ENTRYPOINT ["/docker-entrypoint.sh"]

