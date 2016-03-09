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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

/**
 * http://axibase.com/atsd/api/#messages:-insert
 *
 * @author Nikolay Malevanny.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Message {
    @JsonProperty("entity")
    private String entityName;
    private String date;
    private Long timestamp;
    private String message;
    private Severity severity;
    private String type;
    private String source;
    private Map<String, String> tags;

    public Message() {
    }

    public Message(String entityName, String message) {
        this.entityName = entityName;
        this.message = message;
    }

    public String getEntityName() {
        return entityName;
    }

    public Message setEntityName(String entityName) {
        this.entityName = entityName;
        return this;
    }

    public String getDate() {
        return date;
    }

    public Message setDate(String date) {
        this.date = date;
        return this;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public Message setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    public String getMessage() {
        return message;
    }

    public Message setMessage(String message) {
        this.message = message;
        return this;
    }

    public Severity getSeverity() {
        return severity;
    }

    public Message setSeverity(Severity severity) {
        this.severity = severity;
        return this;
    }

    public String getType() {
        return type;
    }

    public Message setType(String type) {
        this.type = type;
        return this;
    }

    public String getSource() {
        return source;
    }

    public Message setSource(String source) {
        this.source = source;
        return this;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public Message setTags(Map<String, String> tags) {
        this.tags = tags;
        return this;
    }
}
