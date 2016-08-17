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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;


@JsonIgnoreProperties(ignoreUnknown = true)
public class EntityGroup {
    @JsonProperty
    private String name;
    @JsonProperty
    private String expression;
    @JsonProperty
    private Map<String, String> tags;

    public EntityGroup() {
    }

    public EntityGroup(String name) {
        this.name = name;
    }

    /**
     * @return Entity group name (unique).
     */
    public String getName() {
        return name;
    }

    public EntityGroup setName(String name) {
        this.name = name;
        return this;

    }

    /**
     * @return Entity group expression.
     */
    public String getExpression() {
        return expression;
    }

    public EntityGroup setExpression(String expression) {
        this.expression = expression;
        return this;

    }

    /**
     * @return Entity group tags.
     */
    public Map<String, String> getTags() {
        return tags;
    }

    public EntityGroup setTags(Map<String, String> tags) {
        this.tags = tags;
        return this;

    }

    @JsonIgnore
    public EntityGroup setTags(String... tagNamesAndValues) {
        setTags(AtsdUtil.toMap(tagNamesAndValues));
        return this;
    }

    @Override
    public String toString() {
        return "EntityGroup{" +
                "name='" + name + '\'' +
                ", expression='" + expression + '\'' +
                ", tags=" + tags +
                '}';
    }
}
