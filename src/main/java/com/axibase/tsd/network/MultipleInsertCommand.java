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

import static com.axibase.tsd.util.AtsdUtil.formatMetricValue;


public class MultipleInsertCommand extends AbstractInsertCommand {
    private final Map<String, Double> numericValues;
    private final Map<String, String> textValues;

    public MultipleInsertCommand(String entityName, long time, Map<String, String> tags,
                                 Map<String, Double> numericValues) {
        this(entityName, time, tags, numericValues, Collections.<String, String>emptyMap());
    }

    public MultipleInsertCommand(String entityName, long time, Map<String, String> tags,
                                 Map<String, Double> numericValues, Map<String, String> textValues) {
        super(InsertCommand.SERIES_COMMAND, entityName, time, tags);
        this.numericValues = numericValues == null ? Collections.<String, Double>emptyMap() : numericValues;
        this.textValues = textValues == null ? Collections.<String, String>emptyMap() : textValues;
    }

    @Override
    protected void appendValues(StringBuilder sb) {
        for (Map.Entry<String, Double> metricNameAndValue : numericValues.entrySet()){
            sb.append(" m:").append(metricNameAndValue.getKey())
                    .append('=').append(formatMetricValue(metricNameAndValue.getValue()));
        }

        appendKeysAndValues(sb, " x:", textValues);
    }
}
