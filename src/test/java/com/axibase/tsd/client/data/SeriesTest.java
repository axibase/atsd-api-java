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
package com.axibase.tsd.client.data;

import com.axibase.tsd.RerunRule;
import com.axibase.tsd.client.AtsdServerException;
import com.axibase.tsd.client.DataService;
import com.axibase.tsd.client.SeriesCommandPreparer;
import com.axibase.tsd.model.data.TimeFormat;
import com.axibase.tsd.model.data.command.AddSeriesCommand;
import com.axibase.tsd.model.data.command.BatchResponse;
import com.axibase.tsd.model.data.command.GetSeriesQuery;
import com.axibase.tsd.model.data.command.SendCommandResult;
import com.axibase.tsd.model.data.command.SimpleAggregateMatcher;
import com.axibase.tsd.model.data.series.*;
import com.axibase.tsd.model.data.series.aggregate.AggregateType;
import com.axibase.tsd.model.system.Format;
import com.axibase.tsd.network.InsertCommand;
import com.axibase.tsd.network.MultipleInsertCommand;
import com.axibase.tsd.network.PlainCommand;
import com.axibase.tsd.network.SimpleCommand;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.*;

import java.io.InputStream;
import java.util.*;

import static com.axibase.tsd.TestUtil.*;
import static org.junit.Assert.*;

public class SeriesTest extends BaseDataTest {

    @Rule
    public RerunRule rerunRule = new RerunRule();

    @Test
    public void testRetrieveSeries() throws Exception {
        final String entityName = buildVariablePrefix() + "entity";
        final String metricName = buildVariablePrefix() + "metric";
        final int intervalSize = 20;

        GetSeriesQuery getSeriesQuery = new GetSeriesQuery(entityName, metricName);
        getSeriesQuery.setInterval(new Interval(intervalSize, IntervalUnit.DAY));

        List getSeriesResultList = dataService.retrieveSeries(getSeriesQuery);

        if (getSeriesResultList.isEmpty() || ((Series) getSeriesResultList.get(0)).getData().isEmpty()) {
            AddSeriesCommand addSeriesCommand = new AddSeriesCommand(entityName, metricName);
            addSeriesCommand.addSeries(new Sample(System.currentTimeMillis(), MOCK_SERIE_NUMERIC_VALUE, MOCK_SERIE_TEXT_VALUE));
            assertTrue(dataService.addSeries(addSeriesCommand));
            Thread.sleep(WAIT_TIME);
        }

        getSeriesResultList = dataService.retrieveSeries(getSeriesQuery);
        assertEquals(1, getSeriesResultList.size());
        assertTrue(getSeriesResultList.get(0) instanceof Series);
        assertEquals(1, ((Series) getSeriesResultList.get(0)).getData().size());
    }

    @Test
    public void testRetrieveSeriesWithoutDate() throws Exception {
        final String entityName = buildVariablePrefix() + "entity";
        final String metricName = buildVariablePrefix() + "metric";
        final int intervalSize = 20;

        GetSeriesQuery getSeriesQuery = new GetSeriesQuery(entityName, metricName);
        getSeriesQuery.setAggregateMatcher(new SimpleAggregateMatcher(new Interval(intervalSize, IntervalUnit.SECOND),
                Interpolate.NONE,
                AggregateType.DETAIL));

        try {
            dataService.retrieveSeries(getSeriesQuery);
            fail();
        } catch (AtsdServerException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testRetrieveSeriesWithDate() throws Exception {
        final String entityName = buildVariablePrefix() + "entity";
        final String metricName = buildVariablePrefix() + "metric";
        final Long timestamp = MOCK_TIMESTAMP;
        AddSeriesCommand addSeriesCommand = new AddSeriesCommand(entityName, metricName);
        addSeriesCommand.addSeries(new Sample(timestamp, MOCK_SERIE_NUMERIC_VALUE, MOCK_SERIE_TEXT_VALUE));
        assertTrue(dataService.addSeries(addSeriesCommand));
        Thread.sleep(WAIT_TIME);

        GetSeriesQuery getSeriesQuery = new GetSeriesQuery(entityName, metricName)
                .setTimeFormat(TimeFormat.ISO)
                .setStartTime(0L)
                .setEndTime(timestamp + MOCK_TIMESTAMP_DELTA);


        List getSeriesResults = dataService.retrieveSeries(getSeriesQuery);
        assertFalse(getSeriesResults.isEmpty());
        assertTrue(getSeriesResults.get(0) instanceof Series);

        List<Sample> sampleList = ((Series) getSeriesResults.get(0)).getData();
        assertFalse(sampleList.isEmpty());

        Sample s = sampleList.get(0);
        assertTrue(StringUtils.isNoneBlank(s.getIsoDate()));
        assertEquals(MOCK_SERIE_NUMERIC_VALUE, s.getNumericValueAsDouble(), 0);
        assertEquals(MOCK_SERIE_TEXT_VALUE, s.getTextValue());
    }

    @Test
    public void testRetrieveSeriesWithoutTimes() throws Exception {
        final String entityName = buildVariablePrefix() + "entity";
        final String metricName = buildVariablePrefix() + "metric";
        AddSeriesCommand addSeriesCommand = new AddSeriesCommand(entityName, metricName);
        addSeriesCommand.addSeries(new Sample(MOCK_TIMESTAMP, MOCK_SERIE_NUMERIC_VALUE, MOCK_SERIE_TEXT_VALUE));
        assertTrue(dataService.addSeries(addSeriesCommand));

        {
            GetSeriesQuery getSeriesQuery = new GetSeriesQuery(entityName, metricName);
            try {
                dataService.retrieveSeries(getSeriesQuery);
                fail();
            } catch (AtsdServerException e) {
                e.printStackTrace();
            }
        }


    }

    //TODO: add tests to check start\end\interval behavior if anyone does not exist

    @Test
    public void testInsertSeries() throws Exception {
        final String entityName = buildVariablePrefix() + "entity";
        final String metricName = buildVariablePrefix() + "metric";
        final long timestamp = System.currentTimeMillis();
        final int testSerieCount = 10;

        AddSeriesCommand addSeriesCommand = new AddSeriesCommand(entityName, metricName);

        for (int i = 0; i < testSerieCount; i++) {
            addSeriesCommand.addSeries(new Sample(timestamp + i, i, "text" + i));
        }

        dataService.addSeries(addSeriesCommand);

        Thread.sleep(3000);

        {
            GetSeriesQuery getSeriesQuery = new GetSeriesQuery(entityName, metricName).setStartTime(timestamp).setEndTime(timestamp + testSerieCount);
            List getSeriesResultList = dataService.retrieveSeries(getSeriesQuery);
            assertFalse(getSeriesResultList.isEmpty());
            assertTrue(getSeriesResultList.get(0) instanceof Series);
            assertEquals(1, getSeriesResultList.size());
            assertEquals(10, ((Series) getSeriesResultList.get(0)).getData().size());
        }
    }

    @Test
    public void testTcpInsertSimpleCommand() throws InterruptedException {
        final String entityName = buildVariablePrefix() + "entity";
        final String metricName = buildVariablePrefix() + "metric";
        final long timestamp = System.currentTimeMillis();

        PlainCommand command = new SimpleCommand(
                String.format("series e:%s m:%s=1 ms:%s", entityName, metricName, timestamp));

        tcpClientManager.send(command);

        Thread.sleep(500);

        GetSeriesQuery getSeriesQuery = new GetSeriesQuery(entityName, metricName).setStartTime(timestamp).setEndTime(timestamp + 1);
        List getSeriesResultList = dataService.retrieveSeries(getSeriesQuery);
        assertFalse(getSeriesResultList.isEmpty());
        assertTrue(getSeriesResultList.get(0) instanceof Series);
        assertEquals(1, getSeriesResultList.size());
        assertEquals(1, ((Series) getSeriesResultList.get(0)).getData().size());
    }

    @Test
    public void testTcpInsertMultipleSimpleCommands() throws InterruptedException {
        final String entityName = buildVariablePrefix() + "entity";
        final String metricName = buildVariablePrefix() + "metric";
        final long timestamp = System.currentTimeMillis();
        final int commandsCount = 10;

        List<PlainCommand> commands = new ArrayList<>(commandsCount);
        for (int i = 0; i < commandsCount; i++) {
            commands.add(new SimpleCommand(
                    String.format("series e:%s m:%s=1 ms:%s", entityName, metricName, timestamp + i)));
        }
        tcpClientManager.send(commands);

        Thread.sleep(500);

        GetSeriesQuery getSeriesQuery = new GetSeriesQuery(entityName, metricName)
                .setStartTime(timestamp)
                .setEndTime(timestamp + commandsCount);
        List getSeriesResultList = dataService.retrieveSeries(getSeriesQuery);
        assertFalse(getSeriesResultList.isEmpty());
        assertTrue(getSeriesResultList.get(0) instanceof Series);
        assertEquals(1, getSeriesResultList.size());
        assertEquals(commandsCount, ((Series) getSeriesResultList.get(0)).getData().size());
    }

    @Test
    public void testTcpInsertCommand() throws InterruptedException {
        final String entityName = buildVariablePrefix() + "entity";
        final String metricName = buildVariablePrefix() + "metric";
        final long timestamp = System.currentTimeMillis();

        PlainCommand command = new InsertCommand(
                entityName,
                metricName,
                new Sample(timestamp, 1.0, "text"));

        tcpClientManager.send(command);

        Thread.sleep(500);

        GetSeriesQuery getSeriesQuery = new GetSeriesQuery(entityName, metricName).setStartTime(timestamp).setEndTime(timestamp + 1);
        List getSeriesResultList = dataService.retrieveSeries(getSeriesQuery);
        assertFalse(getSeriesResultList.isEmpty());
        assertTrue(getSeriesResultList.get(0) instanceof Series);
        assertEquals(1, getSeriesResultList.size());
        assertEquals(1, ((Series) getSeriesResultList.get(0)).getData().size());
    }

    @Test
    public void testTcpInsertMultipleCommands() throws InterruptedException {
        final String entityName = buildVariablePrefix() + "entity";
        final String metricName = buildVariablePrefix() + "metric";
        final long timestamp = System.currentTimeMillis();
        final int commandsCount = 10;

        List<PlainCommand> commands = new ArrayList<>(commandsCount);
        for (int i = 0; i < commandsCount; i++) {
            commands.add(new InsertCommand(
                    entityName,
                    metricName,
                    new Sample(timestamp + i, 1.0, "text")));
        }

        tcpClientManager.send(commands);

        Thread.sleep(500);

        GetSeriesQuery getSeriesQuery = new GetSeriesQuery(entityName, metricName)
                .setStartTime(timestamp)
                .setEndTime(timestamp + commandsCount);
        List getSeriesResultList = dataService.retrieveSeries(getSeriesQuery);
        assertFalse(getSeriesResultList.isEmpty());
        assertTrue(getSeriesResultList.get(0) instanceof Series);
        assertEquals(1, getSeriesResultList.size());
        assertEquals(commandsCount, ((Series) getSeriesResultList.get(0)).getData().size());
    }

    @Test
    public void testStreamingSendInsertCommand() throws InterruptedException {
        final String entityName = buildVariablePrefix() + "entity";
        final String metricName = buildVariablePrefix() + "metric";
        final long timestamp = System.currentTimeMillis();
        final int commandsCount = 10;

        assertTrue(dataService.canSendPlainCommand());

        for (int i = 0; i < commandsCount; i++) {
            dataService.sendPlainCommand(new InsertCommand(
                    entityName,
                    metricName,
                    new Sample(timestamp + i, 1.0, "text")));
        }

        Thread.sleep(3000);

        GetSeriesQuery getSeriesQuery = new GetSeriesQuery(entityName, metricName)
                .setStartTime(timestamp)
                .setEndTime(timestamp + commandsCount);
        List getSeriesResultList = dataService.retrieveSeries(getSeriesQuery);
        assertFalse(getSeriesResultList.isEmpty());
        assertTrue(getSeriesResultList.get(0) instanceof Series);
        assertEquals(1, getSeriesResultList.size());
        assertEquals(commandsCount, ((Series) getSeriesResultList.get(0)).getData().size());
    }

    @Test
    public void testStreamingSendSimpleCommand() throws InterruptedException {
        final String entityName = buildVariablePrefix() + "entity";
        final String metricName = buildVariablePrefix() + "metric";
        final long timestamp = System.currentTimeMillis();
        final int commandsCount = 10;

        assertTrue(dataService.canSendPlainCommand());

        for (int i = 0; i < commandsCount; i++) {
            dataService.sendPlainCommand(new SimpleCommand(
                    String.format("series e:%s m:%s=1 ms:%s", entityName, metricName, timestamp + i)));
        }

        Thread.sleep(3000);

        GetSeriesQuery getSeriesQuery = new GetSeriesQuery(entityName, metricName)
                .setStartTime(timestamp)
                .setEndTime(timestamp + commandsCount);
        List getSeriesResultList = dataService.retrieveSeries(getSeriesQuery);
        assertFalse(getSeriesResultList.isEmpty());
        assertTrue(getSeriesResultList.get(0) instanceof Series);
        assertEquals(1, getSeriesResultList.size());
        assertEquals(commandsCount, ((Series) getSeriesResultList.get(0)).getData().size());
    }

    @Test
    public void testInsertSeriesCsv() throws Exception {
        final String entityName = buildVariablePrefix() + "entity";
        final String metricName = buildVariablePrefix() + "metric";
        final long timestamp = System.currentTimeMillis();
        StringBuilder sBuilder = new StringBuilder("time, ").append(metricName).append('\n');
        final int testCnt = 10;
        for (int i = 0; i < testCnt; i++) {
            sBuilder.append(timestamp + i).append(",").append(i * i * i).append('\n');
        }

        dataService.addSeriesCsv(entityName, sBuilder.toString(), "ttt-tag-1", "ttt-tag-value-1");

        Thread.sleep(3000);

        List<Series> series = dataService.retrieveSeries(
                new SeriesCommandPreparer() {
                    @Override
                    public void prepare(GetSeriesQuery command) {
                        command.setLimit(10);
                        command.setStartTime(timestamp - 100);
                        command.setEndTime(timestamp + testCnt + 100);
                    }
                },
                new GetSeriesQuery(entityName, metricName, toMVM("ttt-tag-1", "ttt-tag-value-1"))
        );
        assertEquals(1, series.size());
        assertEquals(10, series.get(0).getData().size());
    }

    @Test
    public void testQuerySeriesCsv() throws Exception {
        final String entityName = buildVariablePrefix() + "entity";
        final String metricName = buildVariablePrefix() + "metric";
        final long timestamp = System.currentTimeMillis();
        StringBuilder sBuilder = new StringBuilder("time, ").append(metricName).append('\n');
        final int testCnt = 10;
        for (int i = 0; i < testCnt; i++) {
            sBuilder.append(timestamp + i).append(",").append(i * i * i).append('\n');
        }

        dataService.addSeriesCsv(entityName, sBuilder.toString());

        Thread.sleep(2000);

        Map<String, String> tags = new HashMap<>();
        long endTime = timestamp + testCnt;
        Integer limit = 10;
        String entityPattern = buildVariablePrefix() + "*";
        String columns = "entity, metric, time, value";

        InputStream inputStream = null;
        try {
            inputStream = dataService.querySeriesPack(
                    Format.CSV,
                    entityPattern,
                    metricName,
                    tags,
                    timestamp,
                    endTime,
                    null,
                    AggregateType.DETAIL,
                    limit,
                    false,
                    columns
            );
            List lines = IOUtils.readLines(inputStream);
            assertEquals("entity,metric,time,value", lines.get(0));
            assertEquals(11, lines.size());
        } finally {
            IOUtils.closeQuietly(inputStream);
        }
    }

    @Ignore//TODO: enable test when #3965 will be fixed
    @Test
    public void testRetrieveLastSeries() throws Exception {
        final String entityName = buildVariablePrefix() + "entity";
        final String metricName = buildVariablePrefix() + "metric";
        final Long timestamp = System.currentTimeMillis();

        {
            AddSeriesCommand addSeriesCommand = new AddSeriesCommand(entityName, metricName)
                    .addSeries(new Sample(timestamp, MOCK_SERIE_NUMERIC_VALUE, MOCK_SERIE_TEXT_VALUE));

            assertTrue(dataService.addSeries(addSeriesCommand));
        }

        Thread.sleep(WAIT_TIME);

        {
            GetSeriesQuery getSeriesQuery = new GetSeriesQuery(entityName, metricName)
                    .setStartTime(0L)
                    .setEndTime(timestamp);
            List seriesList = dataService.retrieveLastSeries(getSeriesQuery);

            assertFalse(seriesList.isEmpty());

            assertTrue(seriesList.get(0) instanceof Series);
            assertEquals(1, ((Series) seriesList.get(0)).getData().size());
            assertEquals(MOCK_SERIE_NUMERIC_VALUE, ((Series) seriesList.get(0)).getData().get(0).getNumericValueAsDouble(), 0);
            assertEquals(MOCK_SERIE_TEXT_VALUE, ((Series) seriesList.get(0)).getData().get(0).getTextValue());
            assertEquals(timestamp, ((Series) seriesList.get(0)).getData().get(0).getTimeMillis());
        }

    }

    @Test
    public void testSendBatch() throws Exception {
        testSendBatch(false);
    }

    private void testSendBatch(boolean commit) throws Exception {
        final String entityName = buildVariablePrefix() + "entity";
        final String metricName = buildVariablePrefix() + "metric";
        long st = System.currentTimeMillis();
        final ArrayList<PlainCommand> commands = new ArrayList<>();
        commands.add(new InsertCommand(entityName, metricName, Sample.ofTimeDouble(st + 1, 1.0)));
        commands.add(new InsertCommand(entityName, metricName, new Sample(st + 2, 2.0, "text1")));
        commands.add(new InsertCommand(entityName, metricName, new Sample(st + 3, 3.0, "text2"), Collections.singletonMap("tag1", "value1")));
        commands.add(new InsertCommand(entityName, metricName, new Sample(st + 4, 4.0, "text3"), "tag1", "value1"));
        commands.add(new MultipleInsertCommand(entityName, st + 5, Collections.singletonMap("tag1", "value1"), Collections.singletonMap(metricName, 5.0)));
        commands.add(new MultipleInsertCommand(entityName, st + 6, Collections.<String, String>emptyMap(), Collections.singletonMap(metricName, 6.0), Collections.singletonMap(metricName, "text4")));
        final BatchResponse batchResponse = dataService.sendBatch(commands, commit);
        final SendCommandResult sendCommandResult = batchResponse.getResult();
        assertTrue(sendCommandResult.getFail() == 0);
        assertEquals(6, (int) sendCommandResult.getSuccess());

        if (commit) {
            assertNull(sendCommandResult.getError());
            assertNotNull(sendCommandResult.getStored());
            assertEquals(sendCommandResult.getTotal(), sendCommandResult.getStored());
        } else {
            assertNull(sendCommandResult.getStored());
        }

        Thread.sleep(WAIT_TIME);

        final GetSeriesQuery getSeriesQuery = new GetSeriesQuery(entityName, metricName);
        getSeriesQuery.setStartTime(st);
        getSeriesQuery.setEndTime(st + 7);
        final List<Series> seriesResults = dataService.retrieveSeries(getSeriesQuery);
        assertEquals(2, seriesResults.size());

        Series series = seriesResults.get(0);
        assertEquals(3, series.getData().size());
        assertTrue(series.getTags().isEmpty());
        series = seriesResults.get(1);
        assertEquals(3, series.getData().size());
        assertFalse(series.getTags().isEmpty());
    }

    @Test
    public void testSendBatchWithOutCompression() throws Exception {
        httpClientManager.close();

        httpClientManager = buildHttpClientManager(false);
        httpClientManager.setCheckPeriodMillis(1000);
        dataService = new DataService();
        dataService.setHttpClientManager(httpClientManager);

        waitWorkingServer(httpClientManager);

        testSendBatch(false);
    }

    @Test
    public void testSendBatchWithCommitParameter() throws Exception {
        httpClientManager.close();

        httpClientManager = buildHttpClientManager(false);
        httpClientManager.setCheckPeriodMillis(1000);
        dataService = new DataService();
        dataService.setHttpClientManager(httpClientManager);

        waitWorkingServer(httpClientManager);

        testSendBatch(true);
    }

    @Test
    public void testSendSeriesWithNaN() throws Exception {
        final String entityName = buildVariablePrefix() + "entity";
        final String metricName = buildVariablePrefix() + "metric";
        long st = System.currentTimeMillis();
        final ArrayList<PlainCommand> commands = new ArrayList<>();
        commands.add(new InsertCommand(entityName, metricName, Sample.ofTimeDouble(st, Double.NaN)));
        commands.add(new MultipleInsertCommand(entityName, st + 1, Collections.<String, String>emptyMap(), Collections.singletonMap(metricName, Double.NaN)));
        final BatchResponse batchResponse = dataService.sendBatch(commands);
        assertTrue(batchResponse.getResult().getFail() == 0);
        assertNull(batchResponse.getResult().getStored());

        Thread.sleep(WAIT_TIME);

        final GetSeriesQuery getSeriesQuery = new GetSeriesQuery(entityName, metricName);
        getSeriesQuery.setStartTime(st);
        getSeriesQuery.setEndTime(st + 2);
        final List<Series> seriesResults = dataService.retrieveSeries(getSeriesQuery);
        assertEquals(2, seriesResults.get(0).getData().size());

        Sample sample = seriesResults.get(0).getData().get(0);
        assertEquals(Double.NaN, sample.getNumericValueAsDouble(), 0);
        assertNull(sample.getTextValue());

        sample = seriesResults.get(0).getData().get(1);
        assertEquals(Double.NaN, sample.getNumericValueAsDouble(), 0);
        assertNull(sample.getTextValue());
    }

    @Test
    public void testSendSeriesWithInfinity() throws Exception {
        final String entityName = buildVariablePrefix() + "entity";
        final String metricName = buildVariablePrefix() + "metric";
        long st = System.currentTimeMillis();
        final ArrayList<PlainCommand> commands = new ArrayList<>();
        commands.add(new InsertCommand(entityName, metricName, Sample.ofTimeDouble(st, Double.POSITIVE_INFINITY)));
        commands.add(new InsertCommand(entityName, metricName, Sample.ofTimeDouble(st + 1, Double.NEGATIVE_INFINITY)));
        Map<String, Double> values = new HashMap<>();
        values.put(metricName, Double.POSITIVE_INFINITY);
        values.put(metricName + "_1", Double.NEGATIVE_INFINITY);
        commands.add(new MultipleInsertCommand(entityName, st + 2, Collections.<String, String>emptyMap(), values));
        final BatchResponse batchResponse = dataService.sendBatch(commands);
        assertTrue(batchResponse.getResult().getFail() == 0);
        assertNull(batchResponse.getResult().getStored());

        Thread.sleep(WAIT_TIME);

        GetSeriesQuery getSeriesQuery = new GetSeriesQuery(entityName, metricName);
        getSeriesQuery.setStartTime(st);
        getSeriesQuery.setEndTime(st + 3);
        List<Series> seriesResults = dataService.retrieveSeries(getSeriesQuery);
        assertEquals(3, seriesResults.get(0).getData().size());

        Sample sample = seriesResults.get(0).getData().get(0);
        assertEquals(Double.NaN, sample.getNumericValueAsDouble(), 0);
        assertNull(sample.getTextValue());

        sample = seriesResults.get(0).getData().get(1);
        assertEquals(Double.NaN, sample.getNumericValueAsDouble(), 0);
        assertNull(sample.getTextValue());

        sample = seriesResults.get(0).getData().get(2);
        assertEquals(Double.NaN, sample.getNumericValueAsDouble(), 0);
        assertNull(sample.getTextValue());

        getSeriesQuery = new GetSeriesQuery(entityName, metricName + "_1");
        getSeriesQuery.setStartTime(st + 2);
        getSeriesQuery.setEndTime(st + 3);
        seriesResults = dataService.retrieveSeries(getSeriesQuery);
        assertEquals(1, seriesResults.get(0).getData().size());

        sample = seriesResults.get(0).getData().get(0);
        assertEquals(Double.NaN, sample.getNumericValueAsDouble(), 0);
        assertNull(sample.getTextValue());
    }


    @Test
    public void testAggregation() throws InterruptedException {
        final String entityName = buildVariablePrefix() + "entity";
        final String metricName = buildVariablePrefix() + "metric";
        final Long timestamp = MOCK_TIMESTAMP;
        AddSeriesCommand addSeriesCommand = new AddSeriesCommand(entityName, metricName);
        addSeriesCommand
                .addSeries(new Sample(timestamp, MOCK_SERIE_NUMERIC_VALUE, MOCK_SERIE_TEXT_VALUE));
        assertTrue(dataService.addSeries(addSeriesCommand));
        Thread.sleep(WAIT_TIME);

        GetSeriesQuery getSeriesQuery = new GetSeriesQuery(entityName, metricName)
                .setTimeFormat(TimeFormat.ISO)
                .setAggregateMatcher(new SimpleAggregateMatcher(
                        new Interval(1, IntervalUnit.MINUTE),
                        null,
                        AggregateType.AVG
                ))
                .setStartTime(0L)
                .setEndTime(timestamp + MOCK_TIMESTAMP_DELTA);

        List getSeriesResults = dataService.retrieveSeries(getSeriesQuery);
        assertFalse(getSeriesResults.isEmpty());
        assertTrue(getSeriesResults.get(0) instanceof Series);

        List<Sample> sampleList = ((Series) getSeriesResults.get(0)).getData();
        assertFalse(sampleList.isEmpty());

        Sample s = sampleList.get(0);
        assertTrue(StringUtils.isNoneBlank(s.getIsoDate()));
        assertEquals(MOCK_SERIE_NUMERIC_VALUE, s.getNumericValueAsDouble(), 0);
    }
}
