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

import com.axibase.tsd.model.data.Property;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Arrays;
import java.util.List;


@JsonInclude(JsonInclude.Include.NON_NULL)
public class BatchPropertyCommand {
    private final String action;
    private List<Property> properties;
    private List<PropertyMatcher> matchers;

    private BatchPropertyCommand(String action) {
        this.action = action;
    }

    public String getAction() {
        return action;
    }

    public List<Property> getProperties() {
        return properties;
    }


    public List<PropertyMatcher> getMatchers() {
        return matchers;
    }

    public static BatchPropertyCommand createInsertCommand(Property... properties) {
        BatchPropertyCommand insertCommand = new BatchPropertyCommand("insert");
        insertCommand.properties = Arrays.asList(properties);
        return insertCommand;
    }


    public static BatchPropertyCommand createDeleteCommand(PropertyMatcher... propertyMatchers) {
        BatchPropertyCommand insertCommand = new BatchPropertyCommand("delete-match");
        insertCommand.matchers = Arrays.asList(propertyMatchers);
        return insertCommand;
    }
}
