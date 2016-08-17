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
package com.axibase.tsd.model.data.command;

import com.axibase.tsd.model.data.TimeFormat;
import com.axibase.tsd.model.data.series.Interval;
import com.axibase.tsd.model.data.series.Join;
import com.axibase.tsd.model.data.series.Rate;
import com.axibase.tsd.model.data.series.SeriesType;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import java.util.Map;


@JsonInclude(JsonInclude.Include.NON_NULL)
public class GetSeriesQuery {
    private Long startTime = null;
    private Long endTime = null;
    private String startDate;
    private String endDate;
    private Interval interval;
    private Integer limit;
    private Boolean last;
    @JsonProperty("entity")
    private final String entityName;
    @JsonProperty("metric")
    private final String metricName;
    private MultivaluedMap<String, String> tags = new MultivaluedHashMap<>();
    private SeriesType type;
    private Join join;
    private Rate rate;
    @JsonProperty("aggregate")
    private SimpleAggregateMatcher aggregateMatcher;
    private String requestId;
    private TimeFormat timeFormat;

    /**
     * @param entityName an entity's name of the requested time series. User defined values (case insensitive).
     * @param metricName a metric name of the requested time series. User defined values (case insensitive).
     */
    public GetSeriesQuery(String entityName, String metricName) {
        this.entityName = entityName;
        this.metricName = metricName;
    }

    /**
     * @param entityName an entity's name of the requested time series. User defined values (case insensitive).
     * @param metricName a metric name of the requested time series. User defined values (case insensitive).
     * @param tags       Object key is a tag name and a value is an array of possible tag values
     */
    public GetSeriesQuery(String entityName, String metricName, MultivaluedMap<String, String> tags) {
        this.entityName = entityName;
        this.metricName = metricName;
        this.tags = tags;
    }

    /**
     * @param entityName an entity's name of the requested time series. User defined values (case insensitive).
     * @param metricName a metric name of the requested time series. User defined values (case insensitive).
     * @param tags       Object key is a tag name and a value is an array of possible tag values
     */
    public GetSeriesQuery(String entityName, String metricName, Map<String, String> tags) {
        this.entityName = entityName;
        this.metricName = metricName;
        this.tags = new MultivaluedHashMap<String, String>(tags);
    }

    public GetSeriesQuery(String entityName,
                          String metricName,
                          Map<String, String> tags,
                          long startTime,
                          long endTime) {
        this(entityName, metricName, tags);
        this.startTime = startTime;
        this.endTime = endTime;
    }

    public Long getStartTime() {
        return startTime;
    }

    public Long getEndTime() {
        return endTime;
    }

    public String getStartDate() {
        return startDate;
    }

    public String getEndDate() {
        return endDate;
    }

    public Interval getInterval() {
        return interval;
    }

    public Integer getLimit() {
        return limit;
    }

    public Boolean getLast() {
        return last;
    }

    public String getEntityName() {
        return entityName;
    }

    public String getMetricName() {
        return metricName;
    }

    public MultivaluedMap<String, String> getTags() {
        return tags;
    }

    public SeriesType getType() {
        return type;
    }

    public Join getJoin() {
        return join;
    }

    public Rate getRate() {
        return rate;
    }

    public SimpleAggregateMatcher getAggregateMatcher() {
        return aggregateMatcher;
    }

    public String getRequestId() {
        return requestId;
    }

    public TimeFormat getTimeFormat() {
        return timeFormat;
    }

    /**
     * @param startTime start of the selection interval. Unix milliseconds.
     * @return instance of method
     */
    public GetSeriesQuery setStartTime(Long startTime) {
        this.startTime = startTime;
        return this;
    }

    /**
     * @param endTime end of the selection interval. Unix milliseconds.
     * @return instance of method
     */
    public GetSeriesQuery setEndTime(Long endTime) {
        this.endTime = endTime;
        return this;
    }

    /**
     * @param startDate start of the selection interval. Specified in ISO format or using endtime syntax.
     * @return instance of method
     */
    public GetSeriesQuery setStartDate(String startDate) {
        this.startDate = startDate;
        return this;
    }

    /**
     * @param endDate end of the selection interval. Specified in ISO format or using endtime syntax.
     * @return instance of method
     */
    public GetSeriesQuery setEndDate(String endDate) {
        this.endDate = endDate;
        return this;
    }

    /**
     * @param interval Duration of the selection interval, specified as {@code count-timeunit}, for example, 1-hour
     * @return instance of method
     */
    public GetSeriesQuery setInterval(Interval interval) {
        this.interval = interval;
        return this;
    }

    /**
     * @param limit maximum number of data samples returned.
     * @return instance of method
     */
    public GetSeriesQuery setLimit(Integer limit) {
        this.limit = limit;
        return this;
    }

    /**
     * @param last if true: Performs GET instead of scan. Retrieves only 1 most recent value.
     * @return instance of method
     */
    public GetSeriesQuery setLast(Boolean last) {
        this.last = last;
        return this;
    }

    /**
     * @param tags Object key is a tag name and a value is an array of possible tag values.
     *             User defined values ( keys: case insensitive, values: case sensitive )
     * @return instance of method
     */
    public GetSeriesQuery setTags(MultivaluedMap<String, String> tags) {
        this.tags = tags;
        return this;
    }

    /**
     * @param type specifies source for underlying data
     * @return instance of method
     */
    public GetSeriesQuery setType(SeriesType type) {
        this.type = type;
        return this;
    }

    /**
     * @param join Merges multiple time series into one serie.
     * @return instance of method
     */
    public GetSeriesQuery setJoin(Join join) {
        this.join = join;
        return this;
    }

    /**
     * @param rate Computes difference between consecutive samples per unit of time (rate interval).
     * @return instance of method
     */
    public GetSeriesQuery setRate(Rate rate) {
        this.rate = rate;
        return this;
    }

    /**
     * @param aggregateMatcher Computes statistics for the specified time intervals.
     * @return instance of method
     */
    public GetSeriesQuery setAggregateMatcher(SimpleAggregateMatcher aggregateMatcher) {
        this.aggregateMatcher = aggregateMatcher;
        return this;
    }

    /**
     * To associate 'series' object (one) in request with 'series' objects (many) in response, the client can
     * optionally specify a unique 'requestId' property in each series object in request.
     * For example, the client can set requestId to series object's index in the request.
     * The server echos requestId for each series in the response.
     *
     * @param requestId Optional identifier used to associate 'series' object in request with 'series'
     *                  objects in response. Any string (case sensitive).
     * @return instance of method
     */
    public GetSeriesQuery setRequestId(String requestId) {
        this.requestId = requestId;
        return this;
    }

    public GetSeriesQuery setTimeFormat(TimeFormat timeFormat) {
        this.timeFormat = timeFormat;
        return this;
    }

    @Override
    public String toString() {
        return "GetSeriesQuery{" +
                "startTime=" + startTime +
                ", endTime=" + endTime +
                ", startDate='" + startDate + '\'' +
                ", endDate='" + endDate + '\'' +
                ", interval='" + interval + '\'' +
                ", limit=" + limit +
                ", last=" + last +
                ", entityName='" + entityName + '\'' +
                ", metricName='" + metricName + '\'' +
                ", tags=" + tags +
                ", type=" + type +
                ", join=" + join +
                ", rate=" + rate +
                ", aggregateMatcher=" + aggregateMatcher +
                ", requestId='" + requestId + '\'' +
                ", timeFormat=" + timeFormat +
                '}';
    }
}
