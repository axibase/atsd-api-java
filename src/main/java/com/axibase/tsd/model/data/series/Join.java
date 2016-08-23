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
package com.axibase.tsd.model.data.series;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * Contains data to merge multiple time series into one series.
 *
 * @author Nikolay Malevanny.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Join {
    private JoinType type;
    private Interpolate interpolate;
    private Boolean truncate;
    private Interval interval;

    public JoinType getType() {
        return type;
    }

    public Interpolate getInterpolate() {
        return interpolate;
    }

    public Boolean getTruncate() {
        return truncate;
    }

    public Interval getInterval() {
        return interval;
    }

    /**
     * @param type Statistical function applied to value array [v-n, w-n]
     */
    public void setType(JoinType type) {
        this.type = type;
    }

    /**
     * @param interpolate Interpolation function used to compute missing values for a given input series at t-n
     */
    public void setInterpolate(Interpolate interpolate) {
        this.interpolate = interpolate;
    }

    /**
     * @param truncate Discards samples at the beginning and at the of the joined series until values for all
     *                 input series are established
     */
    public void setTruncate(Boolean truncate) {
        this.truncate = truncate;
    }

    /**
     * @param interval Replaces input series timestamps with regular timestamps based on count=unit frequency
     */
    public void setInterval(Interval interval) {
        this.interval = interval;
    }
}
