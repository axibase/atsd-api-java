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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.StringUtils;

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
    private double numericValue;
    @JsonProperty("x")
    private String textValue;

    private Sample() {
    }

    public Sample(long timeMillis, double value) {
        this();
        this.timeMillis = timeMillis;
        this.numericValue = value;
    }

    public Sample(long timeMillis, double numericValue, String textValue) {
        this.timeMillis = timeMillis;
        this.numericValue = numericValue;
        if (StringUtils.isNotEmpty(textValue)) {
            this.textValue = textValue;
        }
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

    /**
     * @since 0.5.15
     * @deprecated use {@link #setNumericValue(double)} instead.
     */
    @JsonIgnore
    @Deprecated
    public double getValue() {
        return numericValue;
    }

    /**
     * @since 0.5.15
     * @deprecated use {@link #getNumericValue()} instead.
     */
    @Deprecated
    public void setValue(double value) {
        this.numericValue = value;
    }

    public double getNumericValue() {
        return numericValue;
    }

    public void setNumericValue(double value) {
        this.numericValue = value;
    }

    public String getTextValue() {
        return textValue;
    }

    public void setTextValue(String value) {
        this.textValue = value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Sample)) {
            return false;
        }

        Sample sample = (Sample) o;

        if (Double.compare(sample.numericValue, numericValue) != 0) {
            return false;
        }
        if (timeMillis != null ? !timeMillis.equals(sample.timeMillis) : sample.timeMillis != null) {
            return false;
        }
        if (!date.equals(sample.date)) {
            return false;
        }
        return textValue != null ? textValue.equals(sample.textValue) : sample.textValue == null;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = timeMillis != null ? timeMillis.hashCode() : 0;
        result = 31 * result + date.hashCode();
        temp = Double.doubleToLongBits(numericValue);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + (textValue != null ? textValue.hashCode() : 0);
        return result;
    }
}
