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
package com.axibase.tsd;

import com.axibase.tsd.client.ClientConfigurationFactory;
import com.axibase.tsd.client.DataService;
import com.axibase.tsd.client.HttpClientManager;
import com.axibase.tsd.model.data.Property;
import com.axibase.tsd.model.data.command.GetPropertiesQuery;
import com.axibase.tsd.model.system.ClientConfiguration;
import com.axibase.tsd.plain.PropertyInsertCommand;
import com.axibase.tsd.util.AtsdUtil;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import java.util.List;

import static com.axibase.tsd.util.AtsdUtil.toMap;

/**
 * @author Nikolay Malevanny.
 */
public class TestUtil {
    public static final String TTT_TYPE = "ttt-type";
    public static final String TTT_RULE = "ttt-rule";
    public static final String NNN_TYPE = "java-nnn-type";
    public static final String TTT_ENTITY = "ttt-entity";
    public static final String SSS_ENTITY = "sss-entity";
    public static final String NNN_ENTITY = "nnn-entity3";
    public static final String TTT_METRIC = "ttt-metric";
    public static final String SSS_METRIC = "sss-metric";
    public static final String SSS_TAG = "sss-tag";
    public static final String UUU_TAG = "uuu-tag";
    public static final String YYY_METRIC = "yyy-metric";
    public static final String NNN_METRIC = "java-nnn-metric";
    public static final String TTT_ENTITY_GROUP = "ttt-entity-group";
    public static final String NNN_ENTITY_GROUP = "nnn-entity-group";

    public static final int WAIT_TIME = 1800;

    static final int RERUN_COUNT = 3;
    public static final int MAX_PING_TRIES = 77;

    // To overwrite client properties use Maven properties like:
    // -DargLine="-Daxibase.tsd.api.server.name=10.100.10.5 -Daxibase.tsd.api.server.port=8888"
    public static HttpClientManager buildHttpClientManager() {
        // Use -Daxibase.tsd.api.client.properties=<filename> to change default properties file name
        ClientConfigurationFactory configurationFactory = ClientConfigurationFactory.createInstance();
        ClientConfiguration clientConfiguration = configurationFactory.createClientConfiguration();
        HttpClientManager httpClientManager = new HttpClientManager();
        httpClientManager.setClientConfiguration(clientConfiguration);
        GenericObjectPoolConfig objectPoolConfig = new GenericObjectPoolConfig();
        objectPoolConfig.setMaxTotal(100);
        objectPoolConfig.setMaxIdle(100);
        httpClientManager.setObjectPoolConfig(objectPoolConfig);
        httpClientManager.setBorrowMaxWaitMillis(10000);
        return httpClientManager;
    }

    public static MultivaluedMap<String, String> toMVM(String... tagNamesAndValues) {
        return new MultivaluedHashMap<String, String>(AtsdUtil.toMap(tagNamesAndValues));
    }

    public static void waitWorkingServer(HttpClientManager httpClientManager) throws InterruptedException {
        for (int i = 0; i < MAX_PING_TRIES; i ++) {
            if (httpClientManager.canSendPlainCommand()) {
                return;
            } else {
                Thread.sleep(WAIT_TIME);
            }
        }
    }

    public static List<Property> fixTestDataProperty(DataService ds) throws InterruptedException {
        ds.insertProperties(new Property(TTT_TYPE+".t", TTT_ENTITY,
                AtsdUtil.toMap("key1", "ttt.t-key-1", "key2", "ttt.t-key-2"),
                AtsdUtil.toMap("val1", "ttt.t-key-value-1", "val2", "ttt.t-key-value-3")));
        Thread.sleep(WAIT_TIME);

        // "property type:ttt-type entity:ttt-entity time:111 key:key1=ttt-key-1 key2=ttt-key-2 " +
        // "values: key1=ttt-key-value-1 key2=ttt-key-value-3"
        PropertyInsertCommand command = new PropertyInsertCommand(
                TTT_ENTITY, TTT_TYPE, System.currentTimeMillis() - 1000,
                AtsdUtil.toMap("key1", "ttt-key-1", "key2", "ttt-key-2"),
                AtsdUtil.toMap("val1", "ttt-key-value-1", "val2", "ttt-key-value-3")
        );
        System.out.println("command = " + command.compose());
        ds.sendPlainCommand(command);
        ds.sendPlainCommand(new PropertyInsertCommand(
                TTT_ENTITY, TTT_TYPE+".t", System.currentTimeMillis() - 1000,
                AtsdUtil.toMap("key1", "ttt.t-key-1", "key2", "ttt.t-key-2"),
                AtsdUtil.toMap("val1", "ttt.t-key-value-1", "val2", "ttt.t-key-value-3")
        ));
        Thread.sleep(WAIT_TIME);
        return ds.retrieveProperties(buildPropertiesQuery());
    }

    public static GetPropertiesQuery buildPropertiesQuery() {
        GetPropertiesQuery query = new GetPropertiesQuery(TTT_TYPE, TTT_ENTITY);
        query.setStartTime(0);
        query.setEndTime(Long.MAX_VALUE);
        query.setKey(AtsdUtil.toMap("key1", "ttt-key-1"));
        return query;
    }
}
