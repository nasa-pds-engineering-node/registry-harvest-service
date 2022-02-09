# Harvest Web Service

Server application providing the functionality for capturing and indexing product metadata into the PDS Registry system. 
This application is different from the standalone Harvest Tool. 
It has to be used with other components, such as RabbitMQ message broker, Crawler Server and Harvest Client 
to enable performant ingestion of large data sets. 

## Build
This is a Java application. You need Java 11 JDK and Maven to build it.
To create a binary distribution (ZIP and TGZ archives) run the following maven command:

```
mvn package
``` 

## Documentation
To create maven documentation site, run the following command:

```
mvn site:run
```
Then open this URL in your web browser `http://localhost:8080`

