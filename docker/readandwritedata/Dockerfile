FROM openjdk:18-jdk-slim
COPY ReadAndWriteData.class /usr/src/readandwritedata/
VOLUME [ "/data" ]
WORKDIR /usr/src/readandwritedata

CMD ["java", "ReadAndWriteData"]