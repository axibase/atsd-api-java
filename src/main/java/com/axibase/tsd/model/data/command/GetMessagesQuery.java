/*
 *
 *  * Copyright 2016 Axibase Corporation or its affiliates. All Rights Reserved.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License").
 *  * You may not use this file except in compliance with the License.
 *  * A copy of the License is located at
 *  *
 *  * https://www.axibase.com/atsd/axibase-apache-2.0.pdf
 *  *
 *  * or in the "license" file accompanying this file. This file is distributed
 *  * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  * express or implied. See the License for the specific language governing
 *  * permissions and limitations under the License.
 *
 */
package com.axibase.tsd.model.data.command;

import com.axibase.tsd.model.data.Severity;
import com.axibase.tsd.model.data.series.Interval;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.*;

import static com.axibase.tsd.util.AtsdUtil.DateTime.*;

/**
 * @author Korchagin Dmitry.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class GetMessagesQuery {
    //TODO: entity and date filter
    private Interval interval;
    private Date startDate;
    private Date endDate;
    private String type;
    private String source;
    private Map<String, String> tags;
    private Severity severity;
    private List<Severity> severities;
    private Severity minSeverity;
    private String entity;
    private List<String> entities;
    private String entityGroup;
    private String entityExpression;

    public GetMessagesQuery(String entity) {
        setEntity(entity);
        setStartDate(parseDate(MIN_QUERIED_DATE_TIME));
        setEndDate(parseDate(MAX_QUERIED_DATE_TIME));
    }

    public GetMessagesQuery(List<String> entities) {
        setEntities(entities);
        setStartDate(parseDate(MIN_QUERIED_DATE_TIME));
        setEndDate(parseDate(MAX_QUERIED_DATE_TIME));
    }


    public Interval getInterval() {
        return interval;
    }

    public GetMessagesQuery setInterval(Interval interval) {
        this.interval = interval;
        return this;
    }

    public String getStartDate() {
        return isoFormat(startDate);
    }

    public GetMessagesQuery setStartDate(Date startDate) {
        this.startDate = startDate;
        return this;
    }

    public String getEndDate() {
        return isoFormat(endDate);
    }

    public GetMessagesQuery setEndDate(Date endDate) {
        this.endDate = endDate;
        return this;
    }

    public String getType() {
        return type;
    }

    public GetMessagesQuery setType(String type) {
        this.type = type;
        return this;
    }

    public String getSource() {
        return source;
    }

    public GetMessagesQuery setSource(String source) {
        this.source = source;
        return this;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public GetMessagesQuery setTags(Map<String, String> tags) {
        this.tags = tags;
        return this;
    }

    public Severity getSeverity() {
        return severity;
    }

    public GetMessagesQuery setSeverity(Severity severity) {
        this.severity = severity;
        return this;
    }

    public List<Severity> getSeverities() {
        return severities;
    }

    public GetMessagesQuery setSeverities(List<Severity> severities) {
        this.severities = severities;
        return this;
    }

    public Severity getMinSeverity() {
        return minSeverity;
    }

    public GetMessagesQuery setMinSeverity(Severity minSeverity) {
        this.minSeverity = minSeverity;
        return this;
    }

    public String getEntity() {
        return entity;
    }

    public GetMessagesQuery setEntity(String entity) {
        this.entity = entity;
        return this;
    }

    public List<String> getEntities() {
        return entities;
    }

    public GetMessagesQuery setEntities(List<String> entities) {
        this.entities = entities;
        return this;
    }

    public String getEntityGroup() {
        return entityGroup;
    }

    public GetMessagesQuery setEntityGroup(String entityGroup) {
        this.entityGroup = entityGroup;
        return this;
    }

    public String getEntityExpression() {
        return entityExpression;
    }

    public GetMessagesQuery setEntityExpression(String entityExpression) {
        this.entityExpression = entityExpression;
        return this;
    }


}
