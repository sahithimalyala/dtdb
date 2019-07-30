FROM ubuntu:latest
FROM adoptopenjdk/openjdk8
WORKDIR /app
COPY ./target/com.dtdb.master-0.0.1-SNAPSHOT.jar /app/master.jar
CMD ["java","-jar","/app/master.jar"]