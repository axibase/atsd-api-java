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


@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Property  {
    private String type;
    @JsonProperty("entity")
    private String entityName;
    private Map<String, String> key;
    private Map<String, String> tags;
    private Long timestamp;
    private String date;


    public Property() {

    }

    public Property(String type, String entityName, Map<String, String> key) {
        this(type, entityName, key, null, null);
    }

    public Property(String type, String entityName, Map<String, String> key, Map<String, String> tags) {
        this(type, entityName, key, tags, null);
    }

    public Property(String type, String entityName, Map<String, String> key, Map<String, String> tags, Long timestamp) {
        this.type = type;
        this.entityName = entityName;
        this.key = key;
        this.tags = tags;
        this.timestamp = timestamp;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getEntityName() {
        return entityName;
    }

    public void setEntityName(String entityName) {
        this.entityName = entityName;
    }

    public Map<String, String> getKey() {
        return key;
    }

    public void setKey(Map<String, String> key) {
        this.key = key;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    @Override
    public String toString() {
        return "Property{" +
                "type='" + type + '\'' +
                ", entityName='" + entityName + '\'' +
                ", key=" + key +
                ", tags=" + tags +
                ", timestamp=" + timestamp +
                ", date='" + date + '\'' +
                '}';
    }
}
