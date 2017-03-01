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
import com.axibase.tsd.model.data.filters.DeletePropertyFilter;
import com.axibase.tsd.model.data.series.Series;
import com.axibase.tsd.model.data.series.aggregate.AggregateType;
import com.axibase.tsd.model.system.Format;
import com.axibase.tsd.model.system.ServerError;
import com.axibase.tsd.network.PlainCommand;
import com.axibase.tsd.query.Query;
import com.axibase.tsd.query.QueryPart;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
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
    private static final Logger logger = LoggerFactory.getLogger(DataService.class);
    private static final SeriesCommandPreparer LAST_PREPARER = new LastPreparer();

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
     * @return list of {@code Series}
     */
    public List<Series> retrieveSeries(GetSeriesQuery... seriesQueries) {
        QueryPart<Series> query = new Query<>("series/query");
        return httpClientManager.requestDataList(Series.class, query,
                post(Arrays.asList(seriesQueries)));
    }

    public List<Series> retrieveSeries(SeriesCommandPreparer preparer, GetSeriesQuery... seriesQueries) {
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
        QueryPart<Series> query = new Query<Series>("series")
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
        QueryPart<Series> query = new Query<Series>("series")
                .path("csv")
                .path(entityName, true);
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
     * @return list of {@code Series}
     */
    public List<Series> retrieveLastSeries(GetSeriesQuery... seriesQueries) {
        return retrieveSeries(LAST_PREPARER, seriesQueries);
    }

    /**
     * @param format        CSV or JSON.
     * @param entityName    Filter entities by entity name. Support wildcards and expressions.
     * @param metricName    Metric name.
     * @param tags          entity tags
     * @param startTime     start of the selection interval. Specified in UNIX milliseconds.
     * @param endTime       end of the selection interval. Specified in UNIX milliseconds.
     * @param period        Duration of regular time period for grouping raw values. Specified as count timeunit,
     *                      for example, 1 hour.
     * @param aggregateType Statistical function to compute aggregated values for values in each period
     * @param limit         maximum number of data samples returned. Default value: 0 (unlimited)
     * @param last          Performs GET instead of scan. Retrieves only 1 most recent value. Boolean. Default value: false
     * @param columns       Specify which columns must be included. Possible values: time, date (time in ISO), entity, metric,
     *                      t:{name}, value. Default: time, entity, metric, requested tag names, value
     * @return Sample in specified format as InputStream.
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
                .path(entityName, true)
                .path(metricName, true)
                .param("startTime", startTime)
                .param("endTime", endTime)
                .param("period", period)
                .param("aggregate", aggregateType == null ? null : aggregateType.name().toLowerCase())
                .param("limit", limit)
                .param("last", last)
                .param("columns", columns);
        for (Map.Entry<String, String> tagAndValue : tags.entrySet()) {
            seriesQuery = seriesQuery.param("t:" + tagAndValue.getKey(), tagAndValue.getValue());
        }

        return httpClientManager.requestInputStream(seriesQuery, null);
    }

    /**
     * @param getPropertiesQueries args of queries
     * @return list of {@code Property}
     */
    public List<Property> retrieveProperties(GetPropertiesQuery... getPropertiesQueries) {
        List<GetPropertiesQuery> queriesList = new ArrayList<>();
        Collections.addAll(queriesList, getPropertiesQueries);
        QueryPart<Property> query = new Query<>("properties/query");
        return httpClientManager.requestDataList(Property.class, query,
                post(queriesList));
    }

    /**
     * @param entityName entity name
     * @param typeName   property type name
     * @return properties for entity and type
     */
    public List<Property> retrieveProperties(String entityName, String typeName) {
        checkEntityIsEmpty(entityName);
        checkPropertyTypeIsEmpty(typeName);
        QueryPart<Property> query = new Query<>("properties");
        query = query.path(entityName, true).path("types").path(typeName, true);
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

    public List<Message> retrieveMessages(GetMessagesQuery... getMessagesQueries) {
        QueryPart<Message> query = new Query<>("messages/query");
        return httpClientManager.requestDataList(Message.class, query,
                post(Arrays.asList(getMessagesQueries)));
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


    public boolean deleteProperties(List<DeletePropertyFilter> deletePropertyFilters) {
        QueryPart<Property> query = new Query<>("properties/delete");
        return httpClientManager.updateData(query, post(deletePropertyFilters));
    }

    public boolean deleteProperties(DeletePropertyFilter... deletePropertyFilters) {
        return deleteProperties(Arrays.asList(deletePropertyFilters));
    }

    /**
     * @param metricNames   metric filter, multiple values allowed
     * @param entityNames   entity filter, multiple values allowed
     * @param ruleNames     rule filter, multiple values allowed
     * @param severityIds   severity filter, multiple values allowed
     * @param minSeverityId minimal severity filter
     * @param timeFormat    time format
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

    public List<Alert> retrieveAlerts(GetAlertQuery... alertQueries) {
        QueryPart<Alert> query = new Query<>("/alerts/query");
        return httpClientManager.requestDataList(Alert.class, query, post(Arrays.asList(alertQueries)));
    }

    /**
     * @param getAlertHistoryQuery   command with alert history selection details
     * @param getAlertHistoryQueries alerts history queries
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

    public void sendPlainCommand(PlainCommand plainCommand)
            throws AtsdClientException, AtsdServerException {
        httpClientManager.send(plainCommand);
    }

    public BatchResponse sendBatch(Collection<PlainCommand> commands) {

        QueryPart<BatchResponse> query = new Query<BatchResponse>("command");

        StringBuilder data = new StringBuilder();
        for (PlainCommand command : commands) {
            data.append(command.compose());
        }

        BatchResponse result = httpClientManager.requestData(query, post(data), new BatchResponseExtractor());

        return result;
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
            command.setCache(true);
        }
    }

    private static class BatchResponseExtractor implements ResponseDataExtractor<BatchResponse> {

        @Override
        public BatchResponse extract(Response response) {
            BatchResponse batchResponse = new BatchResponse(response.getStatusInfo());
            if (response.getStatus() == HttpClient.HTTP_STATUS_OK) {
                SendCommandResult sendCommandResult = response.readEntity(SendCommandResult.class);
                batchResponse.setResult(sendCommandResult);
            } else {
                ServerError serverError = HttpClient.buildAndLogServerError(response);
                batchResponse.setServerError(serverError);
            }
            return batchResponse;
        }

    }

}