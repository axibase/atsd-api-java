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

import com.axibase.tsd.model.data.series.Sample;
import com.axibase.tsd.util.AtsdUtil;
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.Map;

import static com.axibase.tsd.util.AtsdUtil.checkMetricIsEmpty;
import static com.axibase.tsd.util.AtsdUtil.formatMetricValue;


public class InsertCommand extends AbstractInsertCommand {
    public static final String SERIES_COMMAND = "series";
    private final String metricName;
    private final Sample sample;

    public InsertCommand(String entityName, String metricName, Sample sample, Map<String, String> tags) {
        super(SERIES_COMMAND, entityName, sample.getTimeMillis(), tags);
        checkMetricIsEmpty(metricName);
        this.metricName = metricName;
        this.sample = sample;
    }

    public InsertCommand(String entityName, String metricName, Sample sample) {
        this(entityName, metricName, sample, Collections.<String, String>emptyMap());
    }

    public InsertCommand(String entityName, String metricName, Sample sample, String... tagNamesAndValues) {
        this(entityName, metricName, sample, AtsdUtil.toMap(tagNamesAndValues));
    }

    @Override
    protected void appendValues(StringBuilder sb) {
        sb.append(" m:").append(handleStringValue(metricName)).append('=').append(formatMetricValue(sample.getNumericValue()));
        if (StringUtils.isNotEmpty(sample.getTextValue())) {
            sb.append(" x:").append(handleStringValue(metricName)).append('=').append(sample.getTextValue());
        }
    }

}
