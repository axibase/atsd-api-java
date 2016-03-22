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
import com.axibase.tsd.model.data.TimeFormat;
import com.axibase.tsd.model.data.series.Interval;
import com.axibase.tsd.model.meta.EntityGroup;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @author Korchagin Dmitry.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class GetMessagesQuery {
    @JsonProperty(value = "entity")
    private final String entityName;
    @JsonProperty(value = "entities")
    private final List<String> entitiesName;
    @JsonProperty(value = "entityGroups")
    private final List<String> entityGroupsName;
    @JsonProperty(value = "excludeGroups")
    private List<String> excludeGroupsName;
    private Long startTime = null;
    private Long endTime = null;
    private Interval interval;
    private TimeFormat timeFormat;
    private Integer limit;
    private Severity severity;
    private String type;
    private String source;
    private Map<String, String> tags;

    public GetMessagesQuery(final String entityName) {
        this(entityName, null, null);
    }

    public GetMessagesQuery(final String[] entitesName) {
        this(null, Arrays.asList(entitesName), null);
    }

    public GetMessagesQuery(final List<EntityGroup> entityGroups) {
        this(null, null, entityGroups);
    }


    protected GetMessagesQuery(String entityName, List<String> entitiesName, List<EntityGroup> entityGroups) {
        this.entityName = entityName;
        this.entitiesName = entitiesName;
        if (null == entityGroups) {
            this.entityGroupsName = null;
            return;
        }
        List<String> entityGroupsName = new ArrayList<>();
        for (EntityGroup entityGroup : entityGroups) {
            entityGroupsName.add(entityGroup.getName());
        }
        this.entityGroupsName = entityGroupsName;
    }

    public String getEntityName() {
        return entityName;
    }

    public List<String> getEntitiesName() {
        return entitiesName;
    }

    public List<String> getEntityGroupsName() {
        return entityGroupsName;
    }

    public List<String> getExcludeGroupsName() {
        return excludeGroupsName;
    }

    public GetMessagesQuery setExcludeGroupsName(List<String> excludeGroupsName) {
        this.excludeGroupsName = excludeGroupsName;
        return this;
    }

    public Long getStartTime() {
        return startTime;
    }

    public GetMessagesQuery setStartTime(Long startTime) {
        this.startTime = startTime;
        return this;
    }

    public Long getEndTime() {
        return endTime;
    }

    public GetMessagesQuery setEndTime(Long endTime) {
        this.endTime = endTime;
        return this;
    }

    public Interval getInterval() {
        return interval;
    }

    public GetMessagesQuery setInterval(Interval interval) {
        this.interval = interval;
        return this;
    }

    public TimeFormat getTimeFormat() {
        return timeFormat;
    }

    public GetMessagesQuery setTimeFormat(TimeFormat timeFormat) {
        this.timeFormat = timeFormat;
        return this;
    }

    public Integer getLimit() {
        return limit;
    }

    public GetMessagesQuery setLimit(Integer limit) {
        this.limit = limit;
        return this;
    }

    public Severity getSeverity() {
        return severity;
    }

    public GetMessagesQuery setSeverity(Severity severity) {
        this.severity = severity;
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

    @Override
    public String toString() {
        return "GetMessagesQuery{" +
                "entityName='" + entityName + '\'' +
                ", entitiesName=" + entitiesName +
                ", entityGroupsName=" + entityGroupsName +
                ", excludeGroupsName=" + excludeGroupsName +
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                ", interval=" + interval +
                ", timeFormat=" + timeFormat +
                ", limit=" + limit +
                ", severity=" + severity +
                ", type='" + type + '\'' +
                ", source='" + source + '\'' +
                ", tags=" + tags +
                '}';
    }
}
