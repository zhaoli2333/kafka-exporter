FROM harbor.ld-hadoop.com/java/oracle-jdk:8u351-centos7
WORKDIR /
RUN ln -sf /usr/share/zoneinfo/Asia/Shanghai /etc/localtime
RUN echo 'Asia/Shanghai' >/etc/timezone
RUN mkdir /logs
RUN echo '' > /logs/gc.log
EXPOSE 8080
EXPOSE 8081
ARG JAR_FILE
COPY ${JAR_FILE} /opt/kafka-exporter-java.jar

ENV JAVA_OPTS="\
-server \
-Xmx2g \
-Xms2g \
-XX:+UseG1GC \
-XX:ActiveProcessorCount=4 \
-XX:MaxGCPauseMillis=50 \
-XX:InitiatingHeapOccupancyPercent=35 \
-XX:G1HeapRegionSize=16M \
-XX:+PrintGC \
-XX:+PrintGCDateStamps \
-Xloggc:/logs/gc-%t.log \
-XX:+UseGCLogFileRotation \
-XX:NumberOfGCLogFiles=5 \
-XX:GCLogFileSize=50m \
-XX:+UnlockExperimentalVMOptions \
-XX:+UseCGroupMemoryLimitForHeap \
-XX:MaxRAMFraction=1 \
-XX:NativeMemoryTracking=detail \
-XX:MaxDirectMemorySize=2g"


ENTRYPOINT java ${JAVA_OPTS} -jar /opt/kafka-exporter-java.jar
