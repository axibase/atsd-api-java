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
package com.axibase.tsd.model.data.series;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;


@JsonIgnoreProperties(ignoreUnknown = true)
public class Series {
    private String requestId;
    @JsonProperty("entity")
    private String entityName;
    @JsonProperty("metric")
    private String metricName;
    private Map<String, String> tags;
    private SeriesType type;
    private Rate rate;
    private Aggregate aggregate;
    private List<Sample> data;

    public Series() {
        setAggregate(new Aggregate());
    }

    public String getRequestId() {
        return requestId;
    }

    public String getEntityName() {
        return entityName;
    }

    public String getMetricName() {
        return metricName;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public SeriesType getType() {
        return type;
    }

    public Rate getRate() {
        return rate;
    }

    public Aggregate getAggregate() {
        return aggregate;
    }

    public List<Sample> getData() {
        return data;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public void setEntityName(String entityName) {
        this.entityName = entityName;
    }

    public void setMetricName(String metricName) {
        this.metricName = metricName;
    }

    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }

    public void setType(SeriesType type) {
        this.type = type;
    }

    public void setRate(Rate rate) {
        this.rate = rate;
    }

    public void setAggregate(Aggregate aggregate) {
        this.aggregate = aggregate;
    }

    public void setData(List<Sample> data) {
        this.data = data;
    }

    public String getTimeSeriesKey() {
        return new StringBuilder("[").
                append("metric=").append(metricName).
                append(",entity=").append(entityName).
                append(",tags=").append(tags).
                append("]").toString();
    }

    @Override
    public String toString() {
        return "Series{" +
                ", requestId='" + requestId + '\'' +
                ", entityName='" + entityName + '\'' +
                ", metricName='" + metricName + '\'' +
                ", tags=" + tags +
                ", type=" + type +
                ", rate=" + rate +
                ", aggregate=" + aggregate +
                ", data=" + data +
                '}';
    }
}
