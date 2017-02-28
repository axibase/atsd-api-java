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
package com.axibase.tsd.util;

import com.fasterxml.jackson.databind.util.ISO8601Utils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import javax.ws.rs.core.MediaType;
import java.text.ParseException;
import java.text.ParsePosition;
import java.util.*;


public class AtsdUtil {
    public static final String JSON = MediaType.APPLICATION_JSON;
    public static final String ADD_COMMAND = "add";
    public static final String DELETE_COMMAND = "delete";
    public static final String MARKER_KEYWORD = "marker ";
    public static final String PING_COMMAND = "ping\n";

    public static Map<String, String> toMap(String... tagNamesAndValues) {
        if (ArrayUtils.isEmpty(tagNamesAndValues)) {
            return Collections.emptyMap();
        }

        if (tagNamesAndValues.length % 2 == 1) {
            throw new IllegalArgumentException("Key without value");
        }

        Map<String, String> result = new HashMap<String, String>();
        for (int i = 0; i < tagNamesAndValues.length; i++) {
            result.put(tagNamesAndValues[i], tagNamesAndValues[++i]);
        }
        return result;
    }

    public static Map<String, Double> toValuesMap(Object... metricNamesAndValues) {
        if (ArrayUtils.isEmpty(metricNamesAndValues)) {
            return Collections.emptyMap();
        }

        if (metricNamesAndValues.length % 2 == 1) {
            throw new IllegalArgumentException("Key without value");
        }

        Map<String, Double> result = new HashMap<String, Double>();
        for (int i = 0; i < metricNamesAndValues.length; i++) {
            result.put((String) metricNamesAndValues[i], (Double) metricNamesAndValues[++i]);
        }
        return result;
    }

    public static void check(String value, String errorMessage) {
        if (StringUtils.isBlank(value)) {
            throw new IllegalArgumentException(errorMessage);
        }
    }

    public static void checkPropertyTypeIsEmpty(String type) {
        check(type, "Type is empty");
    }

    public static void checkEntityIsEmpty(String entityName) {
        check(entityName, "Entity name is empty");
    }

    public static void checkEntityGroupIsEmpty(String entityGroupName) {
        check(entityGroupName, "Entity group name is empty");
    }

    public static void checkMetricIsEmpty(String metricName) {
        check(metricName, "Metric name is empty");
    }

    public static class DateTime {
        public static final String MIN_QUERIED_DATE_TIME = "1000-01-01T00:00:00.000Z";
        public static final String MAX_QUERIED_DATE_TIME = "9999-12-31T23:59:59.999Z";

        public static Date parseDate(String date) {
            try {
                return ISO8601Utils.parse(date, new ParsePosition(0));
            } catch (ParseException e) {
                throw new IllegalStateException(e);
            }
        }

        public static String isoFormat(Date date) {
            return isoFormat(date, "GMT");
        }

        public static String isoFormat(Date date, String timeZoneName) {
            return isoFormat(date, true, timeZoneName);
        }

        public static String isoFormat(Date date, boolean withMillis, String timeZoneName) {
            return ISO8601Utils.format(date, withMillis, TimeZone.getTimeZone(timeZoneName));
        }
    }

    public static String formatMetricValue(double value) {
        if (Double.isNaN(value)) {
            return "NaN";
        }
        return Double.toString(value);
    }

}
