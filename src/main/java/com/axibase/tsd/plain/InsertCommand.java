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

package com.axibase.tsd.plain;

import com.axibase.tsd.model.data.series.Series;
import com.axibase.tsd.util.AtsdUtil;

import java.util.Collections;
import java.util.Map;

import static com.axibase.tsd.util.AtsdUtil.checkMetricIsEmpty;

/**
 * @author Nikolay Malevanny.
 */
public class InsertCommand extends AbstractInsertCommand {
    public static final String SERIES_COMMAND = "series";
    private final String metricName;
    private final Series series;

    public InsertCommand(String entityName, String metricName, Series series, Map<String, String> tags) {
        super(SERIES_COMMAND, entityName, series.getTimeMillis(), tags);
        checkMetricIsEmpty(metricName);
        this.metricName = metricName;
        this.series = series;
    }

    public InsertCommand(String entityName, String metricName, Series series) {
        this(entityName, metricName, series, Collections.<String, String>emptyMap());
    }

    public InsertCommand(String entityName, String metricName, Series series, String... tagNamesAndValues) {
        this(entityName, metricName, series, AtsdUtil.toMap(tagNamesAndValues));
    }

    @Override
    protected void appendValues(StringBuilder sb) {
        sb.append(" m:").append(metricName).append('=').append(series.getValue());
    }

}
