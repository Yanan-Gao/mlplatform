FROM amazoncorretto:8
    
RUN mkdir /opt/mlplatform-api/
COPY target/mlplatform-api.jar /opt/mlplatform-api/mlplatform-api.jar

CMD ["java","-jar","/opt/mlplatform-api/mlplatform-api.jar","--server.port=8083"]
