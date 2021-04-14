#
# Build stage
#
FROM maven:3.6.0-jdk-11-slim AS build
COPY src /usr/home/app/src
COPY pom.xml /usr/home/app
RUN mvn -f /usr/home/app/pom.xml clean package
