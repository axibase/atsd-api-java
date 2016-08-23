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
import com.axibase.tsd.model.data.series.Interval;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

import static com.axibase.tsd.util.AtsdUtil.DateTime.MAX_QUERIED_DATE_TIME;
import static com.axibase.tsd.util.AtsdUtil.DateTime.MIN_QUERIED_DATE_TIME;


@JsonInclude(JsonInclude.Include.NON_NULL)
public class GetAlertQuery {
    @JsonProperty(value = "metrics")
    private List<String> metricNames;
    @JsonProperty(value = "entities")
    private List<String> entityNames;
    @JsonProperty(value = "rules")
    private List<String> ruleNames;
    @JsonProperty(value = "severities")
    private List<Integer> severityIds;
    @JsonProperty(value = "minSeverity")
    private Integer minSeverityId;
    private TimeFormat timeFormat;
    private Long startTime = null;
    private Long endTime = null;
    private String startDate;
    private String endDate;
    private Interval interval;
    private String entity;


    public GetAlertQuery(String entity) {
        setEntity(entity);
        setStartDate(MIN_QUERIED_DATE_TIME);
        setEndDate(MAX_QUERIED_DATE_TIME);
    }


    public GetAlertQuery(List<String> metricNames,
                         List<String> entityNames,
                         List<String> ruleNames,
                         List<Integer> severityIds, Integer minSeverityId, TimeFormat timeFormat) {
        this.metricNames = metricNames;
        this.entityNames = entityNames;
        this.ruleNames = ruleNames;
        this.severityIds = severityIds;
        this.minSeverityId = minSeverityId;
        this.timeFormat = timeFormat;
    }

    public List<String> getMetricNames() {
        return metricNames;
    }

    public List<String> getEntityNames() {
        return entityNames;
    }

    public List<String> getRuleNames() {
        return ruleNames;
    }

    public List<Integer> getSeverityIds() {
        return severityIds;
    }

    public Integer getMinSeverityId() {
        return minSeverityId;
    }

    public TimeFormat getTimeFormat() {
        return timeFormat;
    }

    public GetAlertQuery setMetricNames(List<String> metricNames) {
        this.metricNames = metricNames;
        return this;
    }

    public void setEntityNames(List<String> entityNames) {
        this.entityNames = entityNames;
    }

    public void setRuleNames(List<String> ruleNames) {
        this.ruleNames = ruleNames;
    }

    public void setSeverityIds(List<Integer> severityIds) {
        this.severityIds = severityIds;
    }

    public void setMinSeverityId(Integer minSeverityId) {
        this.minSeverityId = minSeverityId;
    }

    public void setTimeFormat(TimeFormat timeFormat) {
        this.timeFormat = timeFormat;
    }

    public Long getStartTime() {
        return startTime;
    }

    public void setStartTime(Long startTime) {
        this.startTime = startTime;
    }

    public Long getEndTime() {
        return endTime;
    }

    public void setEndTime(Long endTime) {
        this.endTime = endTime;
    }

    public String getStartDate() {
        return startDate;
    }

    public void setStartDate(String startDate) {
        this.startDate = startDate;
    }

    public String getEndDate() {
        return endDate;
    }

    public void setEndDate(String endDate) {
        this.endDate = endDate;
    }

    public Interval getInterval() {
        return interval;
    }

    public void setInterval(Interval interval) {
        this.interval = interval;
    }

    @Override
    public String toString() {
        return "GetAlertQuery{" +
                "metricNames=" + metricNames +
                ", entityNames=" + entityNames +
                ", ruleNames=" + ruleNames +
                ", severityIds=" + severityIds +
                ", minSeverityId=" + minSeverityId +
                ", timeFormat=" + timeFormat +
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                ", startDate='" + startDate + '\'' +
                ", endDate='" + endDate + '\'' +
                ", interval=" + interval +
                '}';
    }

    public String getEntity() {
        return entity;
    }

    public void setEntity(String entity) {
        this.entity = entity;
    }
}
