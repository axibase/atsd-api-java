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
package com.axibase.tsd.client.dataService;

import com.axibase.tsd.RerunRule;
import com.axibase.tsd.TestUtil;
import com.axibase.tsd.client.AtsdServerException;
import com.axibase.tsd.client.DataService;
import com.axibase.tsd.client.HttpClientManager;
import com.axibase.tsd.client.SeriesCommandPreparer;
import com.axibase.tsd.model.data.TimeFormat;
import com.axibase.tsd.model.data.command.AddSeriesCommand;
import com.axibase.tsd.model.data.command.GetSeriesQuery;
import com.axibase.tsd.model.data.command.SimpleAggregateMatcher;
import com.axibase.tsd.model.data.series.*;
import com.axibase.tsd.model.data.series.aggregate.AggregateType;
import com.axibase.tsd.model.system.Format;
import com.axibase.tsd.network.AbstractInsertCommand;
import com.axibase.tsd.network.InsertCommand;
import com.axibase.tsd.network.PlainCommand;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import javax.ws.rs.core.MultivaluedHashMap;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static com.axibase.tsd.TestUtil.*;
import static junit.framework.Assert.*;

public class SeriesTest {

    private DataService dataService;
    private HttpClientManager httpClientManager;

    @Rule
    public RerunRule rerunRule = new RerunRule();

    @Before
    public void setUp() throws Exception {
        httpClientManager = TestUtil.buildHttpClientManager();
        httpClientManager.setCheckPeriodMillis(1000);
//        httpClientManager.setCheckPeriodMillis(30); // to extreme tests
        dataService = new DataService();
        dataService.setHttpClientManager(httpClientManager);

        waitWorkingServer(httpClientManager);
    }

    @Test
    public void testRetrieveSeries() throws Exception {
        final String entityName = buildVariablePrefix() + "entity";
        final String metricName = buildVariablePrefix() + "metric";
        final int intervalSize = 20;

        GetSeriesQuery getSeriesQuery = new GetSeriesQuery(entityName, metricName);
//        getSeriesQuery.setAggregateMatcher(new SimpleAggregateMatcher(new Interval(intervalSize, IntervalUnit.SECOND),
//                Interpolate.NONE,
//                AggregateType.DETAIL));
        getSeriesQuery.setInterval(new Interval(intervalSize, IntervalUnit.DAY));

        List getSeriesResultList = dataService.retrieveSeries(getSeriesQuery);

        if (getSeriesResultList.isEmpty() || ((Series) getSeriesResultList.get(0)).getData().isEmpty()) {
            AddSeriesCommand addSeriesCommand = new AddSeriesCommand(entityName, metricName);
            addSeriesCommand.addSeries(new Sample(System.currentTimeMillis(), MOCK_SERIE_VALUE));
            assertTrue(dataService.addSeries(addSeriesCommand));
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
        final Double seriesValue = MOCK_SERIE_VALUE;
        AddSeriesCommand addSeriesCommand = new AddSeriesCommand(entityName, metricName);
        addSeriesCommand.addSeries(new Sample(timestamp, seriesValue));
        assertTrue(dataService.addSeries(addSeriesCommand));

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
        assertTrue(StringUtils.isNoneBlank(s.getDate()));
    }

    @Test
    public void testRetrieveSeriesWithoutTimes() throws Exception {
        final String entityName = buildVariablePrefix() + "entity";
        final String metricName = buildVariablePrefix() + "metric";
        final Long timestamp = MOCK_TIMESTAMP;
        final Double serieValue = MOCK_SERIE_VALUE;
        AddSeriesCommand addSeriesCommand = new AddSeriesCommand(entityName, metricName);
        addSeriesCommand.addSeries(new Sample(timestamp, serieValue));
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

    //TODO add tests to check start\end\interval behavior if anyone does not exist

    @Test
    public void testInsertSeries() throws Exception {
        final String entityName = buildVariablePrefix() + "entity";
        final String metricName = buildVariablePrefix() + "metric";
        final long timestamp = System.currentTimeMillis();
        final int testSerieCount = 10;

        AddSeriesCommand addSeriesCommand = new AddSeriesCommand(entityName, metricName);

        for (int i = 0; i < testSerieCount; i++) {
            addSeriesCommand.addSeries(new Sample(timestamp + i, i));
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
                new GetSeriesQuery(entityName, metricName, TestUtil.toMVM("ttt-tag-1", "ttt-tag-value-1"))
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
        long startTime = timestamp;
        long endTime = timestamp + testCnt;
        String period = null;
        Integer limit = 10;
        boolean last = false;
        String entityPattern = buildVariablePrefix() + "*";
        String columns = "entity, metric, time, value";

        InputStream inputStream = null;
        try {
            inputStream = dataService.querySeriesPack(Format.CSV, entityPattern, metricName, tags, startTime, endTime, period,
                    AggregateType.DETAIL, limit, last, columns);
            List<String> lines = IOUtils.readLines(inputStream);
            assertEquals("entity,metric,time,value", lines.get(0));
            assertEquals(11, lines.size());
        } finally {
            IOUtils.closeQuietly(inputStream);
        }
    }

    @Test
    public void testRetrieveLastSeries() throws Exception {
        final String entityName = buildVariablePrefix() + "entity";
        final String metricName = buildVariablePrefix() + "metric";
        final Long timestamp = System.currentTimeMillis();

        {
            AddSeriesCommand addSeriesCommand = new AddSeriesCommand(entityName, metricName)
                    .addSeries(new Sample(timestamp, MOCK_SERIE_VALUE));

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
            assertEquals(MOCK_SERIE_VALUE, ((Series) seriesList.get(0)).getData().get(0).getValue());
            assertEquals(timestamp, ((Series) seriesList.get(0)).getData().get(0).getTimeMillis());
        }

    }

    @Test
    public void testSendBatch() throws Exception {
        final String entityName = buildVariablePrefix() + "entity";
        final String metricName = buildVariablePrefix() + "metric";
        long st = System.currentTimeMillis();
        final ArrayList<PlainCommand> commands = new ArrayList<PlainCommand>();
        commands.add(new InsertCommand(entityName, metricName, new Sample(st, 1.0)));
        commands.add(new InsertCommand(entityName, metricName, new Sample(st + 1, 2.0)));
        commands.add(new InsertCommand(entityName, metricName, new Sample(st + 2, 3.0)));
        final boolean result = dataService.sendBatch(commands, false);
        assertTrue(result);

        Thread.sleep(WAIT_TIME);

        final GetSeriesQuery getSeriesQuery = new GetSeriesQuery(entityName, metricName);
        getSeriesQuery.setStartTime(st);
        getSeriesQuery.setEndTime(st + 2 + 1);
        final List<Series> seriesResults = dataService.retrieveSeries(getSeriesQuery);
        assertEquals(3, seriesResults.get(0).getData().size());
    }

    @After
    public void tearDown() throws Exception {
        httpClientManager.close();
    }

    private static class SimpleSeriesSender implements Runnable {
        private static AtomicInteger counter = new AtomicInteger(0);

        private long startMs;

        private final DataService dataService;

        private CountDownLatch latch;

        private int cnt = 1;
        private long sleep = 0;
        private String entityName;
        private String metricName;
        private String tagName;
        private String tagValue;

        public SimpleSeriesSender(long startMs, DataService dataService, CountDownLatch latch) {
            this.startMs = startMs;
            this.dataService = dataService;
            this.latch = latch;
        }

        public SimpleSeriesSender setSeriesParameters(String entityName, String metricName, String tagName, String tagValue) {
            this.entityName = entityName;
            this.metricName = metricName;
            this.tagName = tagName;
            this.tagValue = tagValue;
            return this;
        }

        @Override
        public void run() {
            Sample sample = new Sample(startMs + counter.incrementAndGet(), Math.random());
            AbstractInsertCommand plainCommand = new InsertCommand(entityName, metricName, sample,
                    tagName, tagValue);
//            logger.info(plainCommand.compose());
            for (int i = 0; i < cnt; i++) {
                if (dataService.canSendPlainCommand()) {
                    dataService.sendPlainCommand(plainCommand);
                    if (sleep > 0) {
                        try {
                            Thread.sleep(sleep);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                }
            }
            latch.countDown();
        }

    }

    private int countSeries(long start, String entityName, String metricName, MultivaluedHashMap<String, String> tags) {
        GetSeriesQuery seriesQuery = new GetSeriesQuery(entityName, metricName);
        seriesQuery.setStartTime(start - 1);
        seriesQuery.setEndTime(System.currentTimeMillis());
        seriesQuery.setTags(tags);
        List<Series> series = dataService.retrieveSeries(seriesQuery);
        List<Series> results = series;
        int resCnt = 0;
        for (Series result : results) {
            resCnt += result.getData().size();
        }
        return resCnt;
    }


}