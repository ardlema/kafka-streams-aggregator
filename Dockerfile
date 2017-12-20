FROM anapsix/alpine-java:8_jdk

COPY target/kafka-streams-aggregator-1.0-SNAPSHOT.jar /aggregator.jar
COPY docker/docker-entrypoint.sh /
RUN touch /aggregator.log

ENTRYPOINT ["/docker-entrypoint.sh"]

