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
package com.axibase.tsd.client;

import com.axibase.tsd.model.data.*;
import com.axibase.tsd.model.data.command.*;
import com.axibase.tsd.model.data.series.GetSeriesBatchResult;
import com.axibase.tsd.model.data.series.GetSeriesResult;
import com.axibase.tsd.model.data.series.aggregate.AggregateType;
import com.axibase.tsd.model.system.Format;
import com.axibase.tsd.plain.PlainCommand;
import com.axibase.tsd.query.Query;
import com.axibase.tsd.query.QueryPart;

import java.io.InputStream;
import java.util.*;

import static com.axibase.tsd.client.RequestProcessor.patch;
import static com.axibase.tsd.client.RequestProcessor.post;
import static com.axibase.tsd.util.AtsdUtil.*;

/**
 * Provides high-level API to retrieve and update ATSD Data Objects (time-series, alerts, properties).
 *
 * @author Nikolay Malevanny.
 */
public class DataService {
    public static final SeriesCommandPreparer LAST_PREPARER = new LastPreparer();

    private HttpClientManager httpClientManager;

    public DataService() {
    }

    public DataService(HttpClientManager httpClientManager) {
        this.httpClientManager = httpClientManager;
    }

    public void setHttpClientManager(HttpClientManager httpClientManager) {
        this.httpClientManager = httpClientManager;
    }

    /**
     * @param seriesQueries queries with details, each query property overrides common one in the request parameters
     * @return list of {@code GetSeriesResult}
     */
    public List<GetSeriesResult> retrieveSeries(GetSeriesQuery... seriesQueries) {
        QueryPart<GetSeriesBatchResult> query = new Query<GetSeriesBatchResult>("series");
        GetSeriesBatchResult seriesBatchResult = httpClientManager.requestData(GetSeriesBatchResult.class, query,
                post(new BatchQuery<GetSeriesQuery>(Arrays.asList(seriesQueries))));
        return seriesBatchResult == null ? Collections.<GetSeriesResult>emptyList() : seriesBatchResult.getSeriesResults();
    }

    public List<GetSeriesResult> retrieveSeries(SeriesCommandPreparer preparer, GetSeriesQuery... seriesQueries) {
        if (preparer != null) {
            for (GetSeriesQuery seriesQuery : seriesQueries) {
                preparer.prepare(seriesQuery);
            }
        }
        return retrieveSeries(seriesQueries);
    }

    /**
     * @param addSeriesCommands commands that contains time-series which are added
     * @return true if success
     */
    public boolean addSeries(AddSeriesCommand... addSeriesCommands) {
        for (AddSeriesCommand addSeriesCommand : addSeriesCommands) {
            checkEntityIsEmpty(addSeriesCommand.getEntityName());
            checkMetricIsEmpty(addSeriesCommand.getMetricName());
        }
        QueryPart<GetSeriesResult> query = new Query<GetSeriesResult>("series")
                .path("insert");
        return httpClientManager.updateData(query,
                post(Arrays.asList(addSeriesCommands)));
    }

    /**
     * @param entityName        entity name
     * @param data              CSV as String
     * @param tagNamesAndValues entity tags
     * @return true if success
     */
    public boolean addSeriesCsv(String entityName, String data, String... tagNamesAndValues) {
        checkEntityIsEmpty(entityName);
        check(data, "Data is empty");
        QueryPart<GetSeriesResult> query = new Query<GetSeriesResult>("series")
                .path("csv")
                .path(entityName);
        if (tagNamesAndValues != null) {
            if (tagNamesAndValues.length % 2 == 1) {
                throw new IllegalArgumentException("Tag without value");
            }
            for (int i = 0; i < tagNamesAndValues.length; i++) {
                query = query.param(tagNamesAndValues[i], tagNamesAndValues[++i]);
            }
        }
        return httpClientManager.updateData(query, data);
    }

    /**
     * @param seriesQueries queries with details
     * @return list of {@code GetSeriesResult}
     */
    public List<GetSeriesResult> retrieveLastSeries(GetSeriesQuery... seriesQueries) {
        return retrieveSeries(LAST_PREPARER, seriesQueries);
    }

    /**
     * @param format CSV or JSON.
     * @param entityName Filter entities by entity name. Support wildcards and expressions.
     * @param metricName Metric name.
     * @param tags entity tags
     * @param startTime start of the selection interval. Specified in UNIX milliseconds.
     * @param endTime end of the selection interval. Specified in UNIX milliseconds.
     * @param period Duration of regular time period for grouping raw values. Specified as count timeunit,
     *               for example, 1 hour.
     * @param aggregateType Statistical function to compute aggregated values for values in each period
     * @param limit maximum number of data samples returned. Default value: 0
     * @param last Performs GET instead of scan. Retrieves only 1 most recent value. Boolean. Default value: false
     * @param columns Specify which columns must be included. Possible values: time, date (time in ISO), entity, metric,
     *                t:{name}, value. Default: time, entity, metric, requested tag names, value
     * @return Series in specified format as InputStream.
     */
    public InputStream querySeriesPack(Format format,
                                  String entityName,
                                  String metricName,
                                  Map<String, String> tags,
                                  long startTime,
                                  long endTime,
                                  String period,
                                  AggregateType aggregateType,
                                  Integer limit,
                                  Boolean last,
                                  String columns) {
        QueryPart seriesQuery = new Query("series")
                .path(format.name().toLowerCase())
                .path(entityName)
                .path(metricName)
                .param("startTime", startTime)
                .param("endTime", endTime)
                .param("period", period)
                .param("aggregate", aggregateType==null?null:aggregateType.name().toLowerCase())
                .param("limit", limit)
                .param("last", last)
                .param("columns", columns);
        for (Map.Entry<String, String> tagAndValue : tags.entrySet()) {
            seriesQuery = seriesQuery.param("t:"+tagAndValue.getKey(), tagAndValue.getValue());
        }

        return httpClientManager.requestInputStream(seriesQuery, null);
    }

    /**
     * @param getPropertiesQuery command with property filter parameters
     * @return list of {@code Property}
     */
    public List<Property> retrieveProperties(GetPropertiesQuery getPropertiesQuery, GetPropertiesQuery... getPropertiesQueries) {
        QueryPart<Property> query = new Query<Property>("properties");
        return httpClientManager.requestDataList(Property.class, query,
                post(new BatchQuery<GetPropertiesQuery>(getPropertiesQuery, getPropertiesQueries)));
    }

    /**
     * @param entityName entity name
     * @param typeName   property type name
     * @return properties for entity and type
     */
    public List<Property> retrieveProperties(String entityName, String typeName) {
        checkEntityIsEmpty(entityName);
        check(typeName, "Property type name is empty");
        QueryPart<Property> query = new Query<Property>("properties");
        query = query.path(entityName).path("types").path(typeName);
        return httpClientManager.requestDataList(Property.class, query, null);
    }

    /**
     * @param properties list of {@code Property} to add.
     * @return true if success
     */
    public boolean insertProperties(Property... properties) {
        for (Property property : properties) {
            checkEntityIsEmpty(property.getEntityName());
            checkPropertyTypeIsEmpty(property.getType());
        }
        QueryPart<Property> query = new Query<Property>("properties")
                .path("insert");
        return httpClientManager.updateData(query, post(Arrays.asList(properties)));
    }

    /**
     * @param messages list of {@code Message} to add.
     * @return true if success
     */
    public boolean insertMessages(Message... messages) {
        for (Message message : messages) {
            checkEntityIsEmpty(message.getEntityName());
        }
        QueryPart<Message> query = new Query<Message>("messages")
                .path("insert");
        return httpClientManager.updateData(query, post(Arrays.asList(messages)));
    }

    /**
     * @param batchPropertyCommands list of batch commands to mass update properties
     * @return true if success
     */
    public boolean batchUpdateProperties(BatchPropertyCommand... batchPropertyCommands) {
        QueryPart query = new Query("properties");
        return httpClientManager.updateData(query, patch(batchPropertyCommands));
    }

    /**
     * @param metricNames   metric filter, multiple values allowed
     * @param entityNames   entity filter, multiple values allowed
     * @param ruleNames     rule filter, multiple values allowed
     * @param severityIds   severity filter, multiple values allowed
     * @param minSeverityId minimal severity filter
     * @param timeFormat
     * @return list of {@code Alert}
     */
    public List<Alert> retrieveAlerts(
            List<String> metricNames,
            List<String> entityNames,
            List<String> ruleNames,
            List<Integer> severityIds,
            Integer minSeverityId,
            TimeFormat timeFormat) {
        GetAlertQuery alertQuery = new GetAlertQuery(metricNames, entityNames,
                ruleNames, severityIds, minSeverityId, timeFormat);
        alertQuery.setStartTime(0L);
        alertQuery.setEndTime(System.currentTimeMillis());
        return retrieveAlerts(alertQuery);
    }

    public List<Alert> retrieveAlerts(GetAlertQuery alertQuery, GetAlertQuery... alertQueries) {
        QueryPart<Alert> query = new Query<Alert>("/alerts");
        BatchQuery<GetAlertQuery> batchQuery = new BatchQuery<GetAlertQuery>(alertQuery, alertQueries);
        return httpClientManager.requestDataList(Alert.class, query, post(batchQuery));
    }

    /**
     * @param getAlertHistoryQuery command with alert history selection details
     * @return list of  {@code AlertHistory}
     */
    public List<AlertHistory> retrieveAlertHistory(GetAlertHistoryQuery getAlertHistoryQuery,
                                                   GetAlertHistoryQuery... getAlertHistoryQueries) {
        QueryPart<AlertHistory> query = new Query<AlertHistory>("alerts")
                .path("history");
        return httpClientManager.requestDataList(AlertHistory.class, query,
                post(new BatchQuery<GetAlertHistoryQuery>(getAlertHistoryQuery, getAlertHistoryQueries)));
    }

    public boolean batchUpdateAlerts(BatchAlertCommand... commands) {
        QueryPart query = new Query("alerts");
        return httpClientManager.updateData(query, patch(commands));
    }

    private static QueryPart<Alert> fillParams(QueryPart<Alert> query, String paramName, List<String> paramValueList) {
        if (paramValueList != null) {
            for (String paramValue : paramValueList) {
                query = query.param(paramName, paramValue);
            }
        }
        return query;
    }

    public void sendPlainCommand(PlainCommand plainCommand)
            throws AtsdClientException, AtsdServerException {
        httpClientManager.send(plainCommand);
    }

    public boolean sendBatch(Collection<PlainCommand> commands, boolean streaming) {

        if (commands == null) {
            throw new IllegalArgumentException("Commands is null");
        }

        if (streaming) {
            for (PlainCommand command : commands) {
                sendPlainCommand(command);
            }
            return true;
        } else {
            QueryPart<AlertHistory> query = new Query<AlertHistory>("commands")
                    .path("batch");
            StringBuilder data = new StringBuilder();
            for (PlainCommand command : commands) {
                data.append(command.compose());
            }
            return httpClientManager.updateData(query, data.toString());
        }
    }

    public boolean canSendPlainCommand() {
        return httpClientManager.canSendPlainCommand();
    }

    public List<String> removeSavedPlainCommands() {
        return httpClientManager.removeSavedPlainCommands();
    }

    private static class LastPreparer implements SeriesCommandPreparer {
        @Override
        public void prepare(GetSeriesQuery command) {
            command.setLast(true);
        }
    }
}
