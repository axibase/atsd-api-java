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
import com.axibase.tsd.client.HttpClientManager;
import com.axibase.tsd.model.meta.DataType;
import com.axibase.tsd.model.meta.Metric;
import com.axibase.tsd.model.meta.TimePrecision;
import com.axibase.tsd.model.system.ClientConfiguration;
import com.axibase.tsd.util.AtsdUtil;
import com.fasterxml.jackson.databind.util.ISO8601Utils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import java.text.ParseException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * @author Nikolay Malevanny.
 */
public class TestUtil {
    public static final String ALERTS_METRIC = "alerts-metric";
    public static final Long MOCK_TIMESTAMP = 1456489150000L;
    public static final Long MOCK_TIMESTAMP_DELTA = 60L;
    public static final Double MOCK_SERIE_VALUE = 1d;
    public static final String MIN_QUERIED_DATE_TIME = "1000-01-01T00:00:00.000Z";
    public static final String MAX_QUERIED_DATE_TIME = "9999-12-31T23:59:59.999Z";

    public static final int WAIT_TIME = 1800;

    public static final int RERUN_COUNT = 3;
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
        return new MultivaluedHashMap<>(AtsdUtil.toMap(tagNamesAndValues));
    }

    public static void waitWorkingServer(HttpClientManager httpClientManager) {
        for (int i = 0; i < MAX_PING_TRIES; i++) {
            if (httpClientManager.canSendPlainCommand()) {
                return;
            } else {
                try {
                    Thread.sleep(WAIT_TIME);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static Metric createNewTestMetric(String metricName) {
        return new Metric()
                .setName(metricName)
                .setDataType(DataType.INTEGER)
                .setDescription("test")
                .setEnabled(false)
                .setMaxValue(1D)
                .setMinValue(3D)
                .buildTags(
                        "nnn-tag-1", "nnn-tag-value-1",
                        "nnn-tag-2", "nnn-tag-value-2"
                )
                .setTimePrecision(TimePrecision.SECONDS);
    }

    public static String buildVariablePrefix() {
        String methodName = Thread.currentThread().getStackTrace()[2].getMethodName();
        StringBuilder prefix = new StringBuilder();
        for (int i = 0; i < methodName.length(); i++) {
            Character ch = methodName.charAt(i);
            if (Character.isUpperCase(ch)) {
                prefix.append("-");
            }
            prefix.append(Character.toLowerCase(ch));
        }
        prefix.append(":tst-");
        return prefix.toString();
    }


    public static String isoFormat(Date date) {
        return isoFormat(date, true, "UTC");
    }

    public static String isoFormat(long t) {
        return isoFormat(new Date(t));
    }

    public static String isoFormat(long t, boolean withMillis, String timeZoneName) {
        return isoFormat(new Date(t), withMillis, timeZoneName);
    }

    public static String isoFormat(Date date, boolean withMillis, String timeZoneName) {
        String pattern = (withMillis) ? "yyyy-MM-dd'T'HH:mm:ss.SSSXXX" : "yyyy-MM-dd'T'HH:mm:ssXXX";
        SimpleDateFormat dateFormat = new SimpleDateFormat(pattern);
        dateFormat.setTimeZone(TimeZone.getTimeZone(timeZoneName));
        return dateFormat.format(date);
    }

    public static Date parseDate(String date) {
        Date d = null;
        try {
            d = ISO8601Utils.parse(date, new ParsePosition(0));
        } catch (ParseException e) {
            throw new IllegalStateException(e);
        }
        return d;
    }
}
