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
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;


@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Entity {
    @JsonProperty
    private String name;
    @JsonProperty
    private Boolean enabled;
    @JsonProperty
    private Long lastInsertTime;
    @JsonProperty
    private Map<String, String> tags;

    public Entity() {
    }

    public Entity(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public Entity setName(String name) {
        this.name = name;
        return this;
    }

    public Boolean isEnabled() {
        return enabled;
    }

    public Entity setEnabled(Boolean enabled) {
        this.enabled = enabled;
        return this;

    }

    public Long getLastInsertTime() {
        return lastInsertTime;
    }

    public Entity setLastInsertTime(Long lastInsertTime) {
        this.lastInsertTime = lastInsertTime;
        return this;

    }

    public Map<String, String> getTags() {
        return tags;
    }

    public Entity setTags(Map<String, String> tags) {
        this.tags = tags;
        return this;

    }

    public Entity buildTags(String... tagNamesAndValues) {
        setTags(AtsdUtil.toMap(tagNamesAndValues));
        return this;
    }

    @Override
    public String toString() {
        return "Entity{" +
                "name='" + name + '\'' +
                ", enabled=" + enabled +
                ", lastInsertTime=" + lastInsertTime +
                ", tags=" + tags +
                '}';
    }
}
