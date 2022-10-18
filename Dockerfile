# syntax=docker/dockerfile:1
FROM eclipse-temurin:17
EXPOSE 11500
RUN mkdir /app
COPY app/build/libs/nio-file-server-app*.jar /app/app.jar
CMD [ "java", "-jar", "/app/app.jar" ]





