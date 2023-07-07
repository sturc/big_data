FROM openjdk:18-jdk-slim
COPY ReadAndWriteData.java /usr/src/readandwritedata/
WORKDIR /usr/src/readandwritedata
RUN javac ReadAndWriteData.java
VOLUME [ "/data" ]
CMD ["java", "ReadAndWriteData"]
