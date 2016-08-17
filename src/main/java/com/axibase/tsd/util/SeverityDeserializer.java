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
package com.axibase.tsd.util;

import com.axibase.tsd.model.data.Severity;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class SeverityDeserializer extends JsonDeserializer<Severity> {
    private static Map<String, Severity> codeToSeverity = new HashMap<String, Severity>();

    static {
        for (Severity severity : Severity.values()) {
            codeToSeverity.put(severity.getCode(), severity);
        }
    }

    @Override
    public Severity deserialize(final JsonParser parser, final DeserializationContext context) throws IOException {
        return codeToSeverity.get(parser.getValueAsString());
    }
}