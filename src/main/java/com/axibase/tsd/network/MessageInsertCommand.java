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

import org.apache.commons.lang3.StringUtils;

import java.util.Map;


public class MessageInsertCommand extends AbstractInsertCommand {
    public static final String MESSAGE_COMMAND = "message";
    private final String messageText;

    public MessageInsertCommand(String entityName, Long timeMillis, Map<String, String> tags, String messageText) {
        super(MESSAGE_COMMAND, entityName, timeMillis, tags);
        this.messageText = messageText;
        if ((tags == null || tags.isEmpty()) && StringUtils.isBlank(messageText)) {
            throw new IllegalArgumentException("Either message text or one of the tags is required");
        }
    }

    @Override
    protected void appendValues(StringBuilder sb) {
        //message e:<entity> s:<timestamp> t:<key-1>=<value-2> t:<key-2>=<value-2> m:<message>
        if (StringUtils.isNoneBlank(messageText)) {
            sb.append(" m:").append(handleStringValue(messageText));
        }
    }
}
