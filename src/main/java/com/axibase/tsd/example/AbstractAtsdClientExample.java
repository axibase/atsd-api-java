/*
 * Copyright 2016 Axibase Corporation or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * https://www.axibase.com/atsd/axibase-apache-2.0.pdf
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.axibase.tsd.example;

import com.axibase.tsd.client.ClientConfigurationFactory;
import com.axibase.tsd.client.DataService;
import com.axibase.tsd.client.HttpClientManager;
import com.axibase.tsd.client.MetaDataService;
import com.axibase.tsd.model.data.series.GetSeriesResult;
import com.axibase.tsd.model.data.series.Series;
import com.axibase.tsd.model.system.ClientConfiguration;
import com.fasterxml.jackson.databind.util.ISO8601DateFormat;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.text.DateFormat;
import java.util.Date;
import java.util.List;

//import org.springframework.context.ApplicationContext;
//import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * @author Nikolay Malevanny.
 */
public abstract class AbstractAtsdClientExample {
    protected DataService dataService;
    protected MetaDataService metaDataService;

    public DateFormat getDateFormat() {
        return new ISO8601DateFormat();
    }

    public void setDataService(DataService dataService) {
        this.dataService = dataService;
    }

    public void setMetaDataService(MetaDataService metaDataService) {
        this.metaDataService = metaDataService;
    }

    // Client Configuration -- way 1
    protected void configure() {
        System.out.println("Getting Started with Axibase TSD");
        ClientConfiguration clientConfiguration = ClientConfigurationFactory.createInstance().createClientConfiguration();
        System.out.println("Connecting to ATSD: " + clientConfiguration.getMetadataUrl());
        HttpClientManager httpClientManager = new HttpClientManager(clientConfiguration);
        dataService = new DataService(httpClientManager);
        metaDataService = new MetaDataService(httpClientManager);
    }

    // Client Configuration -- way 2
    protected void pureJavaConfigure() {
        ClientConfigurationFactory configurationFactory = new ClientConfigurationFactory(
                "http", "writeyourownservername.com", 8088, // serverPort
                "/api/v1", "/api/v1",
                "username", "pwd",
                3000, // connectTimeout
                3000, // readTimeout
                600000, // pingTimeout
                false, // ignoreSSLErrors
                false // skipStreamingControl
        );
        ClientConfiguration clientConfiguration = configurationFactory.createClientConfiguration();
        System.out.println("Connecting to ATSD: " + clientConfiguration.getMetadataUrl());
        HttpClientManager httpClientManager = new HttpClientManager(clientConfiguration);

        GenericObjectPoolConfig objectPoolConfig = new GenericObjectPoolConfig();
        objectPoolConfig.setMaxTotal(5);
        objectPoolConfig.setMaxIdle(5);

        httpClientManager.setObjectPoolConfig(objectPoolConfig);
        httpClientManager.setBorrowMaxWaitMillis(1000);

        dataService = new DataService(httpClientManager);
        metaDataService = new MetaDataService(httpClientManager);
    }

    // Client Configuration -- way 3
//    public static void main(String[] args) {
//        Add to your pom.xml:
//        <dependency>
//        <groupId>org.springframework</groupId>
//        <artifactId>spring-context</artifactId>
//        <version>4.0.3.RELEASE</version>
//        </dependency>

//        Uncomment springframework imports and code below
//        ApplicationContext context = new ClassPathXmlApplicationContext("example-beans.xml");
//        AtsdClientWriteExample example =(AtsdClientWriteExample)context.getBean("example");
//        example.writeData();
//        example.printData();
//    }

    protected String toISODate(long time) {
        return getDateFormat().format(new Date(time));
    }

    protected void print(GetSeriesResult getSeriesResult) {
        System.out.println("Time Series Key: " + getSeriesResult.getTimeSeriesKey());
        List<Series> data = getSeriesResult.getData();
        for (Series series : data) {
            long ts = series.getTimeMillis();
            System.out.println(toISODate(ts) + "\t" + series.getValue());
        }
    }
}
