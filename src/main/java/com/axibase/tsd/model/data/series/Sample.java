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

import com.axibase.tsd.util.AtsdUtil;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Date;

import static com.axibase.tsd.util.AtsdUtil.DateTime.parseDate;


@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Sample {
    @JsonProperty("t")
    private Long timeMillis;
    @JsonProperty("d")
    private String date;
    @JsonProperty("v")
    private double value;

    public Sample() {
    }

    public Sample(long timeMillis, double value) {
        setTimeMillis(timeMillis);
        setValue(value);
    }

    public Long getTimeMillis() {
        return timeMillis;
    }

    public void setTimeMillis(Long timeMillis) {
        if (date == null) {
            date = AtsdUtil.DateTime.isoFormat(new Date(timeMillis));
        }
        this.timeMillis = timeMillis;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        if (timeMillis == null) {
            timeMillis = parseDate(date).getTime();
        }
        this.date = date;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        final Sample other = (Sample) obj;
        return this.getValue() == other.getValue();

    }

    @Override
    public String toString() {
        return "Sample{" +
                "timeMillis=" + timeMillis +
                ", date='" + date + '\'' +
                ", value=" + value +
                '}';
    }
}
