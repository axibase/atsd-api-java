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
import com.axibase.tsd.network.*;
import com.axibase.tsd.util.AtsdUtil;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.*;

import javax.ws.rs.core.MultivaluedHashMap;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.axibase.tsd.TestUtil.*;
import static com.axibase.tsd.util.AtsdUtil.toMap;
import static junit.framework.Assert.*;

public class SerieTest {

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

        if(getSeriesResultList.isEmpty() || ((GetSeriesResult)getSeriesResultList.get(0)).getData().isEmpty()) {
            AddSeriesCommand addSeriesCommand = new AddSeriesCommand(entityName, metricName);
            addSeriesCommand.addSeries(new Series(System.currentTimeMillis(), MOCK_SERIE_VALUE));
            assertTrue(dataService.addSeries(addSeriesCommand));
        }

        getSeriesResultList = dataService.retrieveSeries(getSeriesQuery);
        assertEquals(1, getSeriesResultList.size());
        assertTrue(getSeriesResultList.get(0) instanceof GetSeriesResult);
        assertEquals(1, ((GetSeriesResult) getSeriesResultList.get(0)).getData().size());
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
        final Double serieValue = MOCK_SERIE_VALUE;
        AddSeriesCommand addSeriesCommand = new AddSeriesCommand(entityName, metricName);
        addSeriesCommand.addSeries(new Series(timestamp, serieValue));
        assertTrue(dataService.addSeries(addSeriesCommand));

        GetSeriesQuery getSeriesQuery = new GetSeriesQuery(entityName, metricName)
                .setTimeFormat(TimeFormat.ISO)
                .setStartTime(0L)
                .setEndTime(timestamp + MOCK_TIMESTAMP_DELTA);



        List getSeriesResults = dataService.retrieveSeries(getSeriesQuery);
        assertFalse(getSeriesResults.isEmpty());
        assertTrue(getSeriesResults.get(0) instanceof GetSeriesResult);

        List<Series> seriesList = ((GetSeriesResult)getSeriesResults.get(0)).getData();
        assertFalse(seriesList.isEmpty());

        Series s = seriesList.get(0);
        assertNull(s.getTimeMillis());
        assertTrue(StringUtils.isNoneBlank(s.getDate()));
    }

    @Test
    public void testRetrieveSeriesWithoutTimes() throws Exception {
        final String entityName = buildVariablePrefix() + "entity";
        final String metricName = buildVariablePrefix() + "metric";
        final Long timestamp = MOCK_TIMESTAMP;
        final Double serieValue = MOCK_SERIE_VALUE;
        AddSeriesCommand addSeriesCommand = new AddSeriesCommand(entityName, metricName);
        addSeriesCommand.addSeries(new Series(timestamp, serieValue));
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
            addSeriesCommand.addSeries(new Series(timestamp + i, i));
        }

        dataService.addSeries(addSeriesCommand);

        Thread.sleep(3000);

        {
            GetSeriesQuery getSeriesQuery = new GetSeriesQuery(entityName, metricName).setStartTime(timestamp).setEndTime(timestamp + testSerieCount);
            List getSeriesResultList = dataService.retrieveSeries(getSeriesQuery);
            assertFalse(getSeriesResultList.isEmpty());
            assertTrue(getSeriesResultList.get(0) instanceof GetSeriesResult);
            assertEquals(1, getSeriesResultList.size());
            assertEquals(10, ((GetSeriesResult)getSeriesResultList.get(0)).getData().size());
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

        List<GetSeriesResult> getSeriesResults = dataService.retrieveSeries(
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
        assertEquals(1, getSeriesResults.size());
        assertEquals(10, getSeriesResults.get(0).getData().size());
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
                    .addSeries(new Series(timestamp, MOCK_SERIE_VALUE));

            assertTrue(dataService.addSeries(addSeriesCommand));
        }

        Thread.sleep(WAIT_TIME);

        {
            GetSeriesQuery getSeriesQuery = new GetSeriesQuery(entityName, metricName)
                    .setStartTime(0L)
                    .setEndTime(timestamp);
            List seriesList = dataService.retrieveLastSeries(getSeriesQuery);

            assertFalse(seriesList.isEmpty());

            assertTrue(seriesList.get(0) instanceof GetSeriesResult);
            assertEquals(1, ((GetSeriesResult) seriesList.get(0)).getData().size());
            assertEquals(timestamp, ((GetSeriesResult) seriesList.get(0)).getData().get(0).getTimeMillis());
            assertEquals(MOCK_SERIE_VALUE, ((GetSeriesResult) seriesList.get(0)).getData().get(0).getValue());
        }

    }


    @Test
    public void testMultiThreadStreamingCommands() throws Exception {
        final String entityName = buildVariablePrefix() + "entity";
        final String metricName = buildVariablePrefix() + "metric";
//        final int cnt = 50;
        final int cnt = 5;
        final int msgCnt = 100;
//        final int msgCnt = 1000;
        final int msgPause = 10;
        int pauseMs = 10;
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(cnt, cnt, 1000,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<Runnable>(cnt));
        long start = System.currentTimeMillis();
        CountDownLatch latch = new CountDownLatch(cnt);
        String tagName = buildVariablePrefix() +"multi-thread-tag-name";

        String tagValue = buildVariablePrefix() + "multi-thread-tag-value";
        for (int i = 0; i < cnt; i++) {
            Thread.sleep(pauseMs);
            final SimpleSeriesSender command = new SimpleSeriesSender(start, dataService, latch);
            command.cnt = msgCnt;
            command.sleep = msgPause;
            command.setSeriesParameters(entityName, metricName, tagName, tagValue);
            threadPoolExecutor.execute(command);
        }
        try {
            latch.await(10 + cnt * msgCnt * msgPause / 1000, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
            fail();
        }

        Thread.sleep(WAIT_TIME);

        MultivaluedHashMap<String, String> tags = new MultivaluedHashMap<String, String>();
//        for (int i = 0; i < size; i++) {
        tags.add(tagName, tagValue);
//        }
        int resCnt = countSeries(start, entityName, metricName, tags);
        assertEquals(cnt, resCnt);
    }

    @Test
    public void testStreamingCommands() throws Exception {
        final String entityName = buildVariablePrefix() + "entity";
        final String metricName = buildVariablePrefix() + "metric";
        final String tagName = buildVariablePrefix() + "streaming-tag-name";
        final String tagValue = buildVariablePrefix() +"streaming-tag-value";
        final int size = 5;
        final int cnt = 30;
//        final int cnt = 30000;
        int pauseMs = 50;
//        int pauseMs = 0;
        long start = System.currentTimeMillis();
        CountDownLatch latch = new CountDownLatch(cnt);
        for (int i = 0; i < cnt; i++) {
            if (pauseMs > 0) {
                Thread.sleep(pauseMs);
            }
            new SimpleSeriesSender(start, dataService, latch).setSeriesParameters(entityName, metricName, tagName, tagValue).run();
        }

        try {
            latch.await(30, TimeUnit.SECONDS);
            System.out.println(cnt + " commands are sent, time = " + (System.currentTimeMillis() - start) + " ms");
        } catch (InterruptedException e) {
            e.printStackTrace();
            fail();
        }

        Thread.sleep(WAIT_TIME * (1 + cnt / 1000));

        MultivaluedHashMap<String, String> tags = new MultivaluedHashMap<String, String>();
        tags.add(tagName, tagValue);
        int resCnt = countSeries(start, entityName, metricName, tags);
        assertEquals(cnt, resCnt);
    }

    @Test
    public void testSendBatch() throws Exception {
        final String entityName = buildVariablePrefix() + "entity";
        final String metricName = buildVariablePrefix() + "metric";
        long st = System.currentTimeMillis();
        final ArrayList<PlainCommand> commands = new ArrayList<PlainCommand>();
        commands.add(new InsertCommand(entityName, metricName, new Series(st, 1.0)));
        commands.add(new InsertCommand(entityName, metricName, new Series(st + 1, 2.0)));
        commands.add(new InsertCommand(entityName, metricName, new Series(st + 2, 3.0)));
        final boolean result = dataService.sendBatch(commands, false);
        assertTrue(result);

        Thread.sleep(WAIT_TIME);

        final GetSeriesQuery getSeriesQuery = new GetSeriesQuery(entityName, metricName);
        getSeriesQuery.setStartTime(st);
        getSeriesQuery.setEndTime(st + 2 + 1);
        final List<GetSeriesResult> seriesResults = dataService.retrieveSeries(getSeriesQuery);
        assertEquals(3, seriesResults.get(0).getData().size());
    }

    @Ignore
    @Test
    public void testStreamingCommandsUnstableNetwork() throws Exception {
        final String entityName = buildVariablePrefix() + "entity";
        final String metricName = buildVariablePrefix() + "metric";
        final int cnt = 1200;
        int pauseMs = 1000;
        long start = System.currentTimeMillis();
        CountDownLatch latch = new CountDownLatch(cnt);
        List<String> simpleCache = new ArrayList<String>();
        for (int i = 0; i < cnt; i++) {
            Thread.sleep(pauseMs);

            Series series = new Series(start + i * pauseMs, i);
            AbstractInsertCommand plainCommand = new InsertCommand(entityName, metricName, series,
                    "thread", Thread.currentThread().getName());
            if (dataService.canSendPlainCommand()) {
                dataService.sendPlainCommand(plainCommand);
                if (!simpleCache.isEmpty()) {
                    System.out.println("Resend " + simpleCache.size() + " commands");
                    for (final String command : simpleCache) {
                        dataService.sendPlainCommand(new SimpleCommand(command));
                    }
                    simpleCache.clear();
                }
            } else {
                System.out.println("Could not send command, it's added to local cache ");
                simpleCache.add(plainCommand.compose());
                List<String> commands = dataService.removeSavedPlainCommands();
                for (String command : commands) {
                    System.out.println("command = " + command);
                }
                simpleCache.addAll(commands);
            }
        }

        Thread.sleep(WAIT_TIME);

        MultivaluedHashMap<String, String> tags = new MultivaluedHashMap<String, String>();
        tags.add("thread", "main");
        int resCnt = countSeries(start, entityName, metricName, tags);
        assertEquals(cnt, resCnt);
    }

    @Test
    public void testMultipleSeriesStreamingCommands() throws Exception {
        final String entityName = buildVariablePrefix() + "entity";
        final String metricNameFirst = buildVariablePrefix() + "metricFirst";
        final String metricNameSecond = buildVariablePrefix() + "metricSecond";
        MultipleInsertCommand plainCommand = new MultipleInsertCommand(entityName, System.currentTimeMillis(),
                toMap("thread", "current"),
                AtsdUtil.toValuesMap(metricNameSecond, 1.0, metricNameFirst, 2.0)
        );
        System.out.println("plainCommand = " + plainCommand.compose());
        dataService.sendPlainCommand(plainCommand);

        Thread.sleep(WAIT_TIME);

        MultivaluedHashMap<String, String> tags = new MultivaluedHashMap<String, String>();
        tags.add("thread", "current");
        GetSeriesQuery seriesQuery = new GetSeriesQuery(entityName, metricNameSecond);
        seriesQuery.setStartTime(System.currentTimeMillis() - 10000);
        seriesQuery.setEndTime(System.currentTimeMillis() + 1000);
        seriesQuery.setTags(tags);
        GetSeriesQuery seriesQuery2 = new GetSeriesQuery(entityName, metricNameFirst);
        seriesQuery2.setStartTime(System.currentTimeMillis() - 10000);
        seriesQuery2.setEndTime(System.currentTimeMillis() + 1000);
        seriesQuery2.setTags(tags);
        List<GetSeriesResult> getSeriesResults = dataService.retrieveSeries(seriesQuery, seriesQuery2);
        List<GetSeriesResult> results = getSeriesResults;
        assertEquals(2, results.size());
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
            Series series = new Series(startMs + counter.incrementAndGet(), Math.random());
            AbstractInsertCommand plainCommand = new InsertCommand(entityName, metricName, series,
                    tagName, tagValue);
//            System.out.println(plainCommand.compose());
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
        List<GetSeriesResult> getSeriesResults = dataService.retrieveSeries(seriesQuery);
        List<GetSeriesResult> results = getSeriesResults;
        int resCnt = 0;
        for (GetSeriesResult result : results) {
            resCnt += result.getData().size();
        }
        return resCnt;
    }


}