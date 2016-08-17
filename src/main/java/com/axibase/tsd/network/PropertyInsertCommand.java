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

package com.axibase.tsd.network;

import java.util.Collections;
import java.util.Map;

import static com.axibase.tsd.util.AtsdUtil.checkPropertyTypeIsEmpty;


public class PropertyInsertCommand extends AbstractInsertCommand {
    public static final String PROPERTY_COMMAND = "property";
    private final String typeName;
    private final Map<String, String> keys;
    private final Map<String, String> values;

    public PropertyInsertCommand(String entityName, String typeName, Long timeMillis, Map<String, String> keys,
                                 Map<String, String> values) {
        super(PROPERTY_COMMAND, entityName, timeMillis, Collections.<String, String>emptyMap());
        checkPropertyTypeIsEmpty(handleStringValue(typeName));
        this.typeName = typeName;
        this.keys = (keys == null) ? Collections.<String, String>emptyMap() : keys;
        this.values = (values == null) ? Collections.<String, String>emptyMap() : values;
    }

    @Override
    protected void appendValues(StringBuilder sb) {
        // property e:abc001 t:disk k:name=sda v:size=203459 v:fs_type=nfs
        sb.append(" t:").append(typeName);
        appendKeysAndValues(sb, " k:", keys);
        appendKeysAndValues(sb, " v:", values);
    }
}
