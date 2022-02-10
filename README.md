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

This project includes documentation web application (maven site).
The documentation provides PDS Registry architecture overview, 
installation, and operation instructions.

To build and run local documentation web application execute the following maven command:

```
mvn site:run
```
Then open this URL in your web browser `http://localhost:8080`

For more information about running all PDS Registry components in Docker see
https://github.com/NASA-PDS/registry
