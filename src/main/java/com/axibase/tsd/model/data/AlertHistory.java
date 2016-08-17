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
package com.axibase.tsd.model.data;

import com.axibase.tsd.util.SeverityDeserializer;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.util.Map;


@JsonIgnoreProperties(ignoreUnknown = true)
public class AlertHistory {
    private String alert;
    private Long alertDuration;
    private Long alertOpenTime;
    @JsonProperty("entity")
    private String entityName;
    @JsonProperty("metric")
    private String metricName;
    private Long receivedTime;
    private Integer repeatCount;
    @JsonProperty("rule")
    private String ruleName;
    private String ruleExpression;
    private String schedule;
    @JsonDeserialize(using = SeverityDeserializer.class)
    private Severity severity;
    private Map<String, String> tags;
    private Long time;
    private Double value;
    private String window;

    public String getAlert() {
        return alert;
    }

    public void setAlert(String alert) {
        this.alert = alert;
    }

    public Long getAlertDuration() {
        return alertDuration;
    }

    public void setAlertDuration(Long alertDuration) {
        this.alertDuration = alertDuration;
    }

    public Long getAlertOpenTime() {
        return alertOpenTime;
    }

    public void setAlertOpenTime(Long alertOpenTime) {
        this.alertOpenTime = alertOpenTime;
    }

    public String getEntityName() {
        return entityName;
    }

    public void setEntityName(String entityName) {
        this.entityName = entityName;
    }

    public String getMetricName() {
        return metricName;
    }

    public void setMetricName(String metricName) {
        this.metricName = metricName;
    }

    public Long getReceivedTime() {
        return receivedTime;
    }

    public void setReceivedTime(Long receivedTime) {
        this.receivedTime = receivedTime;
    }

    public Integer getRepeatCount() {
        return repeatCount;
    }

    public void setRepeatCount(Integer repeatCount) {
        this.repeatCount = repeatCount;
    }

    public String getRuleName() {
        return ruleName;
    }

    public void setRuleName(String ruleName) {
        this.ruleName = ruleName;
    }

    public String getRuleExpression() {
        return ruleExpression;
    }

    public void setRuleExpression(String ruleExpression) {
        this.ruleExpression = ruleExpression;
    }

    public Severity getSeverity() {
        return severity;
    }

    public void setSeverity(Severity severity) {
        this.severity = severity;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }

    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    public Double getValue() {
        return value;
    }

    public void setValue(Double value) {
        this.value = value;
    }

    public String getWindow() {
        return window;
    }

    public void setWindow(String window) {
        this.window = window;
    }

    public String getSchedule() {
        return schedule;
    }

    public void setSchedule(String schedule) {
        this.schedule = schedule;
    }

    @Override
    public String toString() {
        return "AlertHistory{" +
                "alert='" + alert + '\'' +
                ", alertDuration=" + alertDuration +
                ", alertOpenTime=" + alertOpenTime +
                ", entityName='" + entityName + '\'' +
                ", metricName='" + metricName + '\'' +
                ", receivedTime=" + receivedTime +
                ", repeatCount=" + repeatCount +
                ", ruleName='" + ruleName + '\'' +
                ", ruleExpression='" + ruleExpression + '\'' +
                ", schedule='" + schedule + '\'' +
                ", ruleExpression='" + ruleExpression + '\'' +
                ", severity=" + severity +
                ", tags=" + tags +
                ", time=" + time +
                ", value=" + value +
                ", window='" + window + '\'' +
                '}';
    }
}
