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

import com.axibase.tsd.query.ParamValue;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;


@JsonIgnoreProperties(ignoreUnknown = true)
public class Interval implements ParamValue {
    private int count;
    private IntervalUnit unit;
    private IntervalAlignment align;

    public Interval() {
    }

    public Interval(int count, IntervalUnit unit) {
        this.count = count;
        this.unit = unit;
    }


    public int getCount() {
        return count;
    }

    public IntervalUnit getUnit() {
        return unit;
    }

    @JsonIgnore
    public IntervalAlignment getAlign() {
        return align;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public void setUnit(IntervalUnit unit) {
        this.unit = unit;
    }

    public void setAlign(IntervalAlignment align) {
        this.align = align;
    }

    @Override
    public String toParamValue() {
        return Integer.toString(count) + "-" + unit.name();
    }
}
