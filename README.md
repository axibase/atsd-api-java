[![CircleCI](https://circleci.com/gh/axibase/atsd-api-java.svg?style=svg)](https://circleci.com/gh/axibase/atsd-api-java) [![codebeat badge](https://codebeat.co/badges/0d0339b4-9155-4484-8dc6-9bfdf8cc4d09)](https://codebeat.co/projects/github-com-axibase-atsd-api-java) [![codecov](https://codecov.io/gh/axibase/atsd-api-java/branch/master/graph/badge.svg)](https://codecov.io/gh/axibase/atsd-api-java)  [![](https://maven-badges.herokuapp.com/maven-central/com.axibase/atsd-api-java/badge.svg)](https://mvnrepository.com/artifact/com.axibase/atsd-api-java/) 

# Axibase Time Series Database Client for Java

The **ATSD Client for Java** enables Java developers to build reporting, analytical, and alerting applications that read and write data and metadata from the
[Axibase Time Series Database][atsd].

Get started by downloading the zip file from [GitHub][atsd-zip] or by importing this API client with **Maven**.

```xml
        <dependency>
            <groupId>com.axibase</groupId>
            <artifactId>atsd-api-java</artifactId>
            <version>0.5.9</version>
        </dependency>
```

* [Documentation][atsd-api]
* [Issues][atsd-issues]

## Implemented Methods

The **ATSD Client for Java** provides an easy-to-use client for interfacing with **ATSD** metadata and data REST API services.
It has the ability to read and write time series values, statistics, properties, alerts, and messages.

- Data API
    - Series
        - QUERY
        - INSERT
        - CSV INSERT
    - Properties
        - QUERY
        - INSERT
    - Alerts
        - QUERY
    - Alerts History
        - QUERY

- Metadata API
    - Metrics
        - Get Metrics
        - Get Metric
        - Create/Update Metric
        - Delete Metric  
        - Get Entities and Series Tags for Metric
    - Entities
        - Get Entities
        - Get Entity
        - Create/Update Entity
        - Delete Entity
        - Get Metrics for Entity
    - Entity Groups
        - Get Entity Groups
        - Get Entity Group
        - Create/Update Entity Group
        - Delete Entity Group
        - Entities for Entity Group
        - Add Entities to Entity Group
        - Set (Replace) Entities in Entity Group
        - Delete Entities from Entity Group


## Getting Started
Before you begin installing the **ATSD Client for Java**, you need to install a copy of the [Axibase Time-Series Database][atsd].
Download the latest version of ATSD that is available for your Linux distribution.

Minimum requirements for running the ATSD Client: Java 1.7+.

We recommend installing the ATSD Client for Java by using Maven. Build the ATSD Client with
Maven after checking out the code from GitHub.

```
git clone https://github.com/axibase/atsd-api-java.git
cd atsd-api-java
mvn clean dependency:copy-dependencies compile jar:jar
cd target
java -cp "atsd-api-java-0.5.9.jar:dependency/*" -Daxibase.tsd.api.client.properties=./client.properties com.axibase.tsd.example.AtsdClientWriteExample
```

## Examples

See:
* [AtsdClientReadExample][atsd-read-example]
* [AtsdClientWriteExample][atsd-write-example]

### Client Configuration

#### Option 1
Use `-Daxibase.tsd.api.client.properties=./client.properties`

```java
        ClientConfiguration clientConfiguration = ClientConfigurationFactory
            .createInstance()
            .createClientConfiguration();
        HttpClientManager httpClientManager = new HttpClientManager(clientConfiguration);
        DataService dataService = new DataService(httpClientManager);
        MetaDataService metaDataService = new MetaDataService(httpClientManager);
```

**client.properties** example:
```
        axibase.tsd.api.server.name=atsd_server
        axibase.tsd.api.server.port=8080
        #axibase.tsd.api.server.port=8443
        #axibase.tsd.api.protocol=https
        #axibase.tsd.api.ssl.errors.ignore=true
        axibase.tsd.api.username=username
        axibase.tsd.api.password=pwd
```

Usage
```java
        AtsdClientWriteExample atsdClientWriteExample = new AtsdClientWriteExample();
        atsdClientWriteExample.configure();
        atsdClientWriteExample.writeData();
        atsdClientWriteExample.printData();
```

#### Option 2
Use pure Java.
```java
        ClientConfigurationFactory configurationFactory = new ClientConfigurationFactory(
                "http", "atsd_server", 8080, // serverPort
                "/api/v1", "/api/v1",
                "username", "pwd",
                3000, // connectTimeoutMillis
                3000, // readTimeoutMillis
                600000, // pingTimeout
                false, // ignoreSSLErrors
                false // skipStreamingControl
        );
        ClientConfiguration clientConfiguration = configurationFactory
            .createClientConfiguration();
        System.out.println("Connecting to ATSD: " + clientConfiguration.getMetadataUrl());
        HttpClientManager httpClientManager = new HttpClientManager(clientConfiguration);

        GenericObjectPoolConfig objectPoolConfig = new GenericObjectPoolConfig();
        objectPoolConfig.setMaxTotal(5);
        objectPoolConfig.setMaxIdle(5);

        httpClientManager.setObjectPoolConfig(objectPoolConfig);
        httpClientManager.setBorrowMaxWaitMillis(1000);

        DataService dataService = new DataService(httpClientManager);
        MetaDataService metaDataService = new MetaDataService(httpClientManager);
```

Usage
```java
        AtsdClientWriteExample atsdClientWriteExample = new AtsdClientWriteExample();
        atsdClientWriteExample.pureJavaConfigure();
        atsdClientWriteExample.writeData();
        atsdClientWriteExample.printData();
```


#### Option 3
Use Spring.
See **example-beans.xml**
```xml
        <bean id="example" class="com.axibase.tsd.example.AtsdClientWriteExample"/>
        <bean id="dataService" class="com.axibase.tsd.client.DataService"/>
        <bean id="metaDataService" class="com.axibase.tsd.client.MetaDataService"/>
        <bean id="httpClientManager" class="com.axibase.tsd.client.HttpClientManager"/>
        <bean id="genericObjectPoolConfig"
            class="org.apache.commons.pool2.impl.GenericObjectPoolConfig">
            <property name="maxTotal" value="3"/>
        </bean>
        <bean id="clientConfiguration"
            class="com.axibase.tsd.model.system.ClientConfiguration">
            <constructor-arg name="url" value="http://atsd_server:8080/api/v1"/>
            <constructor-arg name="username" value="username"/>
            <constructor-arg name="password" value="pwd"/>
        </bean>
```

Usage
```java
            ApplicationContext context =
                new ClassPathXmlApplicationContext("example-beans.xml");
            AtsdClientWriteExample example =
                (AtsdClientWriteExample)context.getBean("example");
            example.writeData();
            example.printData();
```

### Metadata Processing
```java
        String metricExample = "jvm_memory_used_percent";
        Metric metric = metaDataService.retrieveMetric(metricExample);
        if (metric == null) {
            System.out.println("Unknown metric: " + metricExample);
            return;
        }
        List<EntityAndTags> entityAndTagsList = metaDataService
            .retrieveEntityAndTags(metric.getName(), null);
        System.out.println("===Metric MetaData===");
        System.out.println("Metric: " + metric);
        for (EntityAndTags entityAndTags : entityAndTagsList) {
            String entityName = entityAndTags.getEntityName();
            System.out.println("\n===Entity MetaData===");
            System.out.println("Entity: " + entityName);
            Map<String, String> tags = entityAndTags.getTags();
            System.out.println("===Tags===");
            for (Map.Entry<String, String> tagAndValue : tags.entrySet()) {
                System.out.println("\t" + tagAndValue.getKey() + " : " + tagAndValue.getValue());
            }
        }
```

### Data Queries
```java
        GetSeriesQuery command = new GetSeriesQuery(entityName, metric.getName(), tags,
            System.currentTimeMillis() - 3600, System.currentTimeMillis());
        command.setAggregateMatcher(
            new SimpleAggregateMatcher(new Interval(1, IntervalUnit.MINUTE),
            Interpolate.NONE,
            AggregateType.DETAIL));
        List<GetSeriesResult> getSeriesResults =
            dataService.retrieveSeries(command);
        for (GetSeriesResult getSeriesResult : getSeriesResults) {
            System.out.println("Time Series Key: "
                + getSeriesResult.getTimeSeriesKey());
            List<Series> data = getSeriesResult.getData();
            for (Series series : data) {
                long ts = series.getT();
                System.out.println(toISODate(ts) + "\t" + series.getV());
            }
        }
```

[atsd]:https://axibase.com/products/axibase-time-series-database/
[atsd-api]:https://github.com/axibase/atsd-docs/blob/master/api/README.md
[atsd-zip]:https://github.com/axibase/atsd-api-java/releases/download/v.0.5.9/atsd-api-java-0.5.9-bin.zip
[atsd-issues]:https://axibase.com/customer-support/
[atsd-read-example]:https://github.com/axibase/atsd-api-java/blob/master/src/main/java/com/axibase/tsd/example/AtsdClientReadExample.java
[atsd-write-example]:https://github.com/axibase/atsd-api-java/blob/master/src/main/java/com/axibase/tsd/example/AtsdClientWriteExample.java
