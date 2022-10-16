# syntax=docker/dockerfile:1
FROM eclipse-temurin:18-jdk
RUN mkdir /app
COPY nio-file-server/build/libs/nio-file-server*.jar /app/app.jar
CMD [ "java", "-jar", "/app/app.jar" ]





