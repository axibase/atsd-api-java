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

import static com.axibase.tsd.util.AtsdUtil.checkEntityIsEmpty;


public abstract class AbstractInsertCommand implements PlainCommand {
    private final String commandName;
    protected final String entityName;
    private final Long timeMillis;
    protected final Map<String, String> tags;

    public AbstractInsertCommand(String commandName, String entityName, Long timeMillis, Map<String, String> tags) {
        this.commandName = commandName;
        checkEntityIsEmpty(entityName);
        this.entityName = entityName;
        this.timeMillis = timeMillis;
        this.tags = tags == null ? Collections.<String, String>emptyMap() : tags;
    }

    @Override
    public final String compose() {
        StringBuilder sb = new StringBuilder(commandName)
                .append(' ').append("e:").append(handleStringValue(entityName));
        if (timeMillis != null) {
            sb.append(' ').append("ms:").append(timeMillis);
        }
        appendKeysAndValues(sb, " t:", tags);
        appendValues(sb);
        return sb.append('\n').toString();
    }

    protected static void appendKeysAndValues(StringBuilder sb, String prefix, Map<String, String> map) {
        for (Map.Entry<String, String> tagNameAndValue : map.entrySet()) {
            sb.append(prefix)
                    .append(tagNameAndValue.getKey())
                    .append('=')
                    .append(handleStringValue(tagNameAndValue.getValue()));
        }
    }

    protected static String handleStringValue(String value) {
        if (value == null) {
            return "null";
        } else {
            return '"' + value.replace("\"", "\"\"") + '"';
        }
    }

    protected abstract void appendValues(StringBuilder sb);
}
