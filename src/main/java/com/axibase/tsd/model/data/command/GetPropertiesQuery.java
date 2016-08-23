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
package com.axibase.tsd.model.data.command;

import com.axibase.tsd.model.data.TimeFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Date;
import java.util.Map;

import static com.axibase.tsd.util.AtsdUtil.DateTime.*;


@JsonInclude(JsonInclude.Include.NON_NULL)
public class GetPropertiesQuery {
    @JsonProperty(value = "entity")
    private final String entityName;
    private final String type;
    private Long startTime = null;
    private Long endTime = null;
    private String limit;
    private boolean last;
    private Map<String, String> key;
    private String keyExpression;
    private TimeFormat timeFormat;


    public GetPropertiesQuery(String type, String entityName, Date startDateTime, Date endDateTime) {
        this.entityName = entityName;
        this.type = type;
        setStartTime(startDateTime.getTime());
        setEndTime(endDateTime.getTime());
    }


    public GetPropertiesQuery(String type, String entityName) {
        this(type, entityName, parseDate(MIN_QUERIED_DATE_TIME), parseDate(MAX_QUERIED_DATE_TIME));
    }

    public String getEntityName() {
        return entityName;
    }

    public String getType() {
        return type;
    }

    public Long getStartTime() {
        return startTime;
    }

    public Long getEndTime() {
        return endTime;
    }

    public String getLimit() {
        return limit;
    }

    public boolean isLast() {
        return last;
    }

    public Map<String, String> getKey() {
        return key;
    }

    public String getKeyExpression() {
        return keyExpression;
    }

    public TimeFormat getTimeFormat() {
        return timeFormat;
    }

    public GetPropertiesQuery setStartTime(long startTime) {
        this.startTime = startTime;
        return this;
    }

    public GetPropertiesQuery setEndTime(long endTime) {
        this.endTime = endTime;
        return this;
    }

    public GetPropertiesQuery setLimit(String limit) {
        this.limit = limit;
        return this;
    }

    public GetPropertiesQuery setLast(boolean last) {
        this.last = last;
        return this;
    }

    public GetPropertiesQuery setKey(Map<String, String> key) {
        this.key = key;
        return this;
    }

    public GetPropertiesQuery setKeyExpression(String keyExpression) {
        this.keyExpression = keyExpression;
        return this;
    }

    public GetPropertiesQuery setTimeFormat(TimeFormat timeFormat) {
        this.timeFormat = timeFormat;
        return this;
    }

    @Override
    public String toString() {
        return "GetPropertiesQuery{" +
                "entityName='" + entityName + '\'' +
                ", type='" + type + '\'' +
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                ", limit='" + limit + '\'' +
                ", last=" + last +
                ", key=" + key +
                ", keyExpression='" + keyExpression + '\'' +
                ", timeFormat=" + timeFormat +
                '}';
    }
}
