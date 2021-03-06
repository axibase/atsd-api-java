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

import com.axibase.tsd.util.BigDecimalDeserializer;
import com.fasterxml.jackson.annotation.*;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Data;
import lombok.experimental.Accessors;

import java.math.BigDecimal;

@Data
/* Use chained setters that return this instead of void */
@Accessors(chain = true)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Sample {
    /* We use Long here because the field can be omitted */
    @JsonProperty("t")
    private Long timeMillis;
    @JsonProperty("d")
    private String isoDate;
    @JsonDeserialize(using = BigDecimalDeserializer.class)
    @JsonInclude(JsonInclude.Include.ALWAYS)
    @JsonProperty("v")
    private BigDecimal numericValue;
    @JsonProperty("x")
    private String textValue;

    @JsonCreator
    private Sample() {}

    private Sample(Long timeMillis, String isoDate,
                  BigDecimal numericValue, String textValue) {
        this.timeMillis = timeMillis;
        this.isoDate = isoDate;
        this.numericValue = numericValue;
        this.textValue = textValue;
    }

    public Sample(long timeMillis, double numericValue, String textValue) {
        this(timeMillis, null, null, textValue);
        setNumericValueFromDouble(numericValue);
    }

    /**
     * Creates a new {@link Sample} with time and double value specified
     *
     * @param  time         time in milliseconds from 1970-01-01 00:00:00
     * @param  numericValue the numeric value of the sample
     * @return the Sample with specified fields
     */
    public static Sample ofTimeDouble(long time, double numericValue) {
        return new Sample(time, null, null, null)
                .setNumericValueFromDouble(numericValue);
    }

    /**
     * Creates a new {@link Sample} with date in ISO 8061 format and double value specified
     *
     * @param  isoDate      date in ISO 8061 format according
     *                      to <a href="https://www.ietf.org/rfc/rfc3339.txt">RFC3339</a>
     * @param  numericValue the numeric value of the sample
     * @return the Sample with specified fields
     */
    public static Sample ofIsoDouble(String isoDate, double numericValue) {
        return new Sample(null, isoDate, null, null)
                .setNumericValueFromDouble(numericValue);
    }

    /**
     * Creates a new {@link Sample} with time, double and text value specified
     *
     * @param  time         time in milliseconds from 1970-01-01 00:00:00
     * @param  numericValue the numeric value of the sample
     * @param  textValue    the text value of the sample
     * @return the Sample with specified fields
     */
    public static Sample ofTimeDoubleText(long time, double numericValue, String textValue) {
        return new Sample(time, null, null, textValue)
                .setNumericValueFromDouble(numericValue);
    }

    /**
     * Creates a new {@link Sample} with date in ISO 8061 format, double and text value specified
     *
     * @param  isoDate      date in ISO 8061 format according
     *                      to <a href="https://www.ietf.org/rfc/rfc3339.txt">RFC3339</a>
     * @param  numericValue the numeric value of the sample
     * @param  textValue    the text value of the sample
     * @return the Sample with specified fields
     */
    public static Sample ofIsoDoubleText(String isoDate, double numericValue, String textValue) {
        return new Sample(null, isoDate, null, textValue)
                .setNumericValueFromDouble(numericValue);
    }

    /**
     * Creates a new {@link Sample} with time and text value specified
     *
     * @param  time         time in milliseconds from 1970-01-01 00:00:00
     * @param  textValue    the text value of the sample
     * @return the Sample with specified fields
     */
    public static Sample ofTimeText(long time, String textValue) {
        return new Sample(time, null, null, textValue);
    }

    /**
     * Creates a new {@link Sample} with date in ISO 8061 format and text value specified
     *
     * @param  isoDate      date in ISO 8061 format according
     *                      to <a href="https://www.ietf.org/rfc/rfc3339.txt">RFC3339</a>
     * @param  textValue    the text value of the sample
     * @return the Sample with specified fields
     */
    public static Sample ofIsoText(String isoDate, String textValue) {
        return new Sample(null, isoDate, null, textValue);
    }

    @JsonIgnore
    public double getNumericValueAsDouble() {
        return numericValue == null ? Double.NaN : numericValue.doubleValue();
    }

    @JsonIgnore
    public Sample setNumericValueFromDouble(double numericValue) {
        if (Double.isNaN(numericValue) || Double.isInfinite(numericValue)) {
            this.numericValue = null;
        } else {
            this.numericValue = BigDecimal.valueOf(numericValue);
        }

        return this;
    }
}
