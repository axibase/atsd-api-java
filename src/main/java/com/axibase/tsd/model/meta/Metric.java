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
package com.axibase.tsd.model.meta;

import com.axibase.tsd.util.AtsdUtil;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Map;


@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Metric {
    private String name;
    private String label;
    private Boolean enabled;
    private DataType dataType;
    private TimePrecision timePrecision;
    private Boolean persistent;
    private String filter;
    private Double minValue;
    private Double maxValue;
    private InvalidAction invalidAction;
    private String description;
    private Integer retentionInterval;
    private Long lastInsertTime;
    private Map<String, String> tags;

    public Metric() {
    }

    public Metric(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public Metric setName(String name) {
        this.name = name;
        return this;

    }

    public String getLabel() {
        return label;
    }

    public Metric setLabel(String label) {
        this.label = label;
        return this;

    }

    public Boolean isEnabled() {
        return enabled;
    }

    public Metric setEnabled(Boolean enabled) {
        this.enabled = enabled;
        return this;

    }

    public DataType getDataType() {
        return dataType;
    }

    public Metric setDataType(DataType dataType) {
        this.dataType = dataType;
        return this;

    }

    public TimePrecision getTimePrecision() {
        return timePrecision;
    }

    public Metric setTimePrecision(TimePrecision timePrecision) {
        this.timePrecision = timePrecision;
        return this;

    }

    public Boolean isPersistent() {
        return persistent;
    }

    public Metric setPersistent(Boolean persistent) {
        this.persistent = persistent;
        return this;

    }

    public String getFilter() {
        return filter;
    }

    public Metric setFilter(String filter) {
        this.filter = filter;
        return this;

    }

    public Double getMinValue() {
        return minValue;
    }

    public Metric setMinValue(Double minValue) {
        this.minValue = minValue;
        return this;

    }

    public Double getMaxValue() {
        return maxValue;
    }

    public Metric setMaxValue(Double maxValue) {
        this.maxValue = maxValue;
        return this;

    }

    public InvalidAction getInvalidAction() {
        return invalidAction;
    }

    public Metric setInvalidAction(InvalidAction invalidAction) {
        this.invalidAction = invalidAction;
        return this;

    }

    public String getDescription() {
        return description;
    }

    public Metric setDescription(String description) {
        this.description = description;
        return this;

    }

    public Integer getRetentionInterval() {
        return retentionInterval;
    }

    public Metric setRetentionInterval(Integer retentionInterval) {
        this.retentionInterval = retentionInterval;
        return this;

    }

    public Long getLastInsertTime() {
        return lastInsertTime;
    }

    public Metric setLastInsertTime(Long lastInsertTime) {
        this.lastInsertTime = lastInsertTime;
        return this;

    }

    public Map<String, String> getTags() {
        return tags;
    }

    public Metric setTags(Map<String, String> tags) {
        this.tags = tags;
        return this;

    }

    public Metric buildTags(String... tagNamesAndValues) {
        setTags(AtsdUtil.toMap(tagNamesAndValues));
        return this;
    }

    @Override
    public String toString() {
        return "Metric{" +
                "name='" + name + '\'' +
                ", label='" + label + '\'' +
                ", enabled=" + enabled +
                ", dataType=" + dataType +
                ", timePrecision=" + timePrecision +
                ", persistent=" + persistent +
                ", filter='" + filter + '\'' +
                ", minValue=" + minValue +
                ", maxValue=" + maxValue +
                ", invalidAction=" + invalidAction +
                ", description='" + description + '\'' +
                ", retentionInterval=" + retentionInterval +
                ", lastInsertTime=" + lastInsertTime +
                ", tags=" + tags +
                '}';
    }
}
