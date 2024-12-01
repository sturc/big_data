FROM amazoncorretto:21
COPY ReadAndWriteData.java /usr/src/readandwritedata/
WORKDIR /usr/src/readandwritedata
RUN javac ReadAndWriteData.java
VOLUME [ "/data" ]
CMD ["java", "ReadAndWriteData"]
