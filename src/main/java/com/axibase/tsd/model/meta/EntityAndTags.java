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


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;


@JsonIgnoreProperties(ignoreUnknown = true)
public class EntityAndTags {
    @JsonProperty("entity")
    private String entityName;
    private Long lastInsertTime;
    @JsonProperty
    private Map<String, String> tags;
    

    public String getEntityName() {
        return entityName;
    }

    public EntityAndTags setEntityName(String entityName) {
        this.entityName = entityName;
        return this;

    }

    public Long getLastInsertTime() {
        return lastInsertTime;
    }

    public EntityAndTags setLastInsertTime(Long lastInsertTime) {
        this.lastInsertTime = lastInsertTime;
        return this;

    }

    public Map<String, String> getTags() {
        return tags;
    }

    public EntityAndTags setTags(Map<String, String> tags) {
        this.tags = tags;
        return this;
    }

    @Override
    public String toString() {
        return "EntityAndTags{" +
                "entityName='" + entityName + '\'' +
                ", lastInsertTime=" + lastInsertTime +
                ", tags=" + tags +
                '}';
    }
}
