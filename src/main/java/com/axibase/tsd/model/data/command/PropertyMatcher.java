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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;


@JsonInclude(JsonInclude.Include.NON_NULL)
public class PropertyMatcher {
    private String type;
    @JsonProperty("entity")
    private String entityName;
    private Map<String, String> key;
    private Long createdBeforeTime;
    private String createdBeforeDate;

    public PropertyMatcher() {
    }

    public PropertyMatcher(String type) {
        this(type, null, null);
    }

    public PropertyMatcher(String type, String entityName) {
        this(type, entityName, null);
    }

    public PropertyMatcher(String type, String entityName, Map<String, String> key) {
        this.type = type;
        this.entityName = entityName;
        this.key = key;
    }

    public String getType() {
        return type;
    }

    public String getEntityName() {
        return entityName;
    }

    public Map<String, String> getKey() {
        return key;
    }

    public Long getCreatedBeforeTime() {
        return createdBeforeTime;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setEntityName(String entityName) {
        this.entityName = entityName;
    }

    public PropertyMatcher setKey(Map<String, String> key) {
        this.key = key;
        return this;
    }

    public void setCreatedBeforeTime(Long createdBeforeTime) {
        this.createdBeforeTime = createdBeforeTime;
    }


    public String getCreatedBeforeDate() {
        return createdBeforeDate;
    }

    public void setCreatedBeforeDate(String createdBeforeDate) {
        this.createdBeforeDate = createdBeforeDate;
    }

    @Override
    public String toString() {
        return "PropertyMatcher{" +
                "type='" + type + '\'' +
                ", entityName='" + entityName + '\'' +
                ", key=" + key +
                ", createdBeforeTime=" + createdBeforeTime +
                '}';
    }
}
