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
        GetSeriesQuery c1 = createTestGetTestCommand();
        List<GetSeriesResult> seriesList = dataService.retrieveSeries(c1);

        assertTrue(seriesList.get(0) instanceof GetSeriesResult);
        assertTrue(seriesList.size() > 0);
    }

    @Test
    public void testRetrieveSeriesWithDate() throws Exception {
        GetSeriesQuery c1 = createTestGetTestCommand();
        c1.setTimeFormat(TimeFormat.ISO);
        c1.setStartTime(0L);
        c1.setEndTime(System.currentTimeMillis() + 100);
        List<GetSeriesResult> seriesList = dataService.retrieveSeries(c1);

        GetSeriesResult getSeriesResult = seriesList.get(0);
        assertTrue(getSeriesResult instanceof GetSeriesResult);
        assertTrue(seriesList.size() > 0);
        Series s = getSeriesResult.getData().get(0);
        assertNull(s.getTimeMillis());
        assertTrue(StringUtils.isNoneBlank(s.getDate()));
    }

    @Test
    public void testRetrieveSeriesWithEndtime() throws Exception {
        GetSeriesQuery c1 = createTestGetTestCommand();
        c1.setStartTime(null);
        c1.setEndTime(null);

        List<GetSeriesResult> seriesList = null;
        try {
            seriesList = dataService.retrieveSeries(c1);
            fail();
        } catch (Exception e) {
        }

        c1.setEndDate("current_hour + 1 * hour");
        c1.setStartDate("current_hour - 5 * year");
        seriesList = dataService.retrieveSeries(c1);

        assertTrue(seriesList.get(0) instanceof GetSeriesResult);
        assertTrue(seriesList.size() > 0);

        c1.setStartDate(null);
        try {
            seriesList = dataService.retrieveSeries(c1);
            fail();
        } catch (Exception e) {
        }

        c1.setInterval(new Interval(5, IntervalUnit.YEAR));
        seriesList = dataService.retrieveSeries(c1);

        assertTrue(seriesList.get(0) instanceof GetSeriesResult);
        assertTrue(seriesList.size() > 0);
    }

    @Test
    public void testInsertSeries() throws Exception {
        final long st = System.currentTimeMillis();
        AddSeriesCommand c1 = new AddSeriesCommand(TTT_ENTITY, TTT_METRIC, "ttt-tag-1", "ttt-tag-value-1");
        int testCnt = 10;
        for (int i = 0; i < testCnt; i++) {
            c1.addSeries(
                    new Series(st + i, i)
            );
        }
        AddSeriesCommand c2 = new AddSeriesCommand(TTT_ENTITY, TTT_METRIC
                , "ttt-tag-1", "ttt-tag-value-1"
                , "ttt-tag-2", "ttt-tag-value-2"
        );
        for (int i = 0; i < testCnt; i++) {
            c2.addSeries(
                    new Series(st + i, i * i)
            );
        }
        dataService.addSeries(c1, c2);

        Thread.sleep(3000);

        List<GetSeriesResult> getSeriesResults = dataService.retrieveSeries(
                new SeriesCommandPreparer() {
                    @Override
                    public void prepare(GetSeriesQuery command) {
//                        command.setAggregateMatcher(new AggregateMatcher(new Interval(20, IntervalUnit.SECOND), Interpolate.NONE, AggregateType.DETAIL));
                        command.setLimit(10);
                        command.setStartTime(st - 100);
                        command.setEndTime(st + 100);
                    }
                },
                new GetSeriesQuery(TTT_ENTITY, TTT_METRIC, TestUtil.toMVM("ttt-tag-1", "ttt-tag-value-1")),
                new GetSeriesQuery(TTT_ENTITY, TTT_METRIC, TestUtil.toMVM(
                        "ttt-tag-1", "ttt-tag-value-1"
                        , "ttt-tag-2", "ttt-tag-value-2"))
        );
        assertEquals(3, getSeriesResults.size());
        assertEquals(10, getSeriesResults.get(0).getData().size());
        assertEquals(10, getSeriesResults.get(1).getData().size());
        assertEquals(10, getSeriesResults.get(2).getData().size());
    }

    @Test
    public void testInsertSeriesCsv() throws Exception {
        final long st = System.currentTimeMillis();
        StringBuilder sb = new StringBuilder("time, ").append(TTT_METRIC).append('\n');
        final int testCnt = 10;
        for (int i = 0; i < testCnt; i++) {
            sb.append(st + i).append(",").append(i * i * i).append('\n');
        }

        dataService.addSeriesCsv(TTT_ENTITY, sb.toString(), "ttt-tag-1", "ttt-tag-value-1");

        Thread.sleep(3000);

        List<GetSeriesResult> getSeriesResults = dataService.retrieveSeries(
                new SeriesCommandPreparer() {
                    @Override
                    public void prepare(GetSeriesQuery command) {
                        command.setLimit(10);
                        command.setStartTime(st - 100);
                        command.setEndTime(st + testCnt + 100);
                    }
                },
                new GetSeriesQuery(TTT_ENTITY, TTT_METRIC, TestUtil.toMVM("ttt-tag-1", "ttt-tag-value-1"))
        );
        assertEquals(1, getSeriesResults.size());
        assertEquals(10, getSeriesResults.get(0).getData().size());
    }

    @Test
    public void testQuerySeriesCsv() throws Exception {
        final long st = System.currentTimeMillis();
        StringBuilder sb = new StringBuilder("time, ").append(TTT_METRIC).append('\n');
        final int testCnt = 10;
        for (int i = 0; i < testCnt; i++) {
            sb.append(st + i).append(",").append(i * i * i).append('\n');
        }

        dataService.addSeriesCsv(TTT_ENTITY, sb.toString(), "ttt-tag-1", "ttt-tag-value-1");

        Thread.sleep(3000);

        Map<String, String> tags = AtsdUtil.toMap("ttt-tag-1", "ttt-tag-value-1");
        long startTime = st - 100;
        long endTime = st + testCnt + 100;
        String period = null;
        Integer limit = 10;
        boolean last = false;
//        String entity = TTT_ENTITY;
        String entity = "ttt*";
        String columns = "entity, metric, time, value";

        InputStream inputStream = null;
        try {
            inputStream = dataService.querySeriesPack(Format.CSV, entity, TTT_METRIC, tags, startTime, endTime, period,
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
        GetSeriesQuery c1 = createTestGetTestCommand();
        c1.setAggregateMatcher(null);
        List seriesList = dataService.retrieveLastSeries(c1);

        assertTrue(seriesList.get(0) instanceof GetSeriesResult);
        assertEquals(1, seriesList.size());
    }





    @Test
    public void testMultiThreadStreamingCommands() throws Exception {
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
        String tagValue = "multi-thread";
        for (int i = 0; i < cnt; i++) {
            Thread.sleep(pauseMs);
            final SimpleSeriesSender command = new SimpleSeriesSender(start, dataService, latch, tagValue);
            command.cnt = msgCnt;
            command.sleep = msgPause;
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
        tags.add(SSS_TAG, tagValue);
//        }
        int resCnt = countSssSeries(start, tags);
        assertEquals(cnt, resCnt);
    }

    @Test
    public void testStreamingCommands() throws Exception {
        String tagValue = "streaming";
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
            new SimpleSeriesSender(start, dataService, latch, tagValue).run();
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
        tags.add(SSS_TAG, tagValue);
        int resCnt = countSssSeries(start, tags);
        assertEquals(cnt, resCnt);
    }

    @Test
    public void testSendBatch() throws Exception {
        final String entityName = "bbb-entity";
        final String metricName = "bbb-metric";
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
        final int cnt = 1200;
        int pauseMs = 1000;
        long start = System.currentTimeMillis();
        CountDownLatch latch = new CountDownLatch(cnt);
        List<String> simpleCache = new ArrayList<String>();
        for (int i = 0; i < cnt; i++) {
            Thread.sleep(pauseMs);

            Series series = new Series(start + i * pauseMs, i);
            AbstractInsertCommand plainCommand = new InsertCommand(SSS_ENTITY, SSS_METRIC, series,
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
        int resCnt = countSssSeries(start, tags);
        assertEquals(cnt, resCnt);
    }

    @Test
    public void testMultipleSeriesStreamingCommands() throws Exception {
        MultipleInsertCommand plainCommand = new MultipleInsertCommand(SSS_ENTITY, System.currentTimeMillis(),
                toMap("thread", "current"),
                AtsdUtil.toValuesMap(SSS_METRIC, 1.0, YYY_METRIC, 2.0)
        );
        System.out.println("plainCommand = " + plainCommand.compose());
        dataService.sendPlainCommand(plainCommand);

        Thread.sleep(3000);

        MultivaluedHashMap<String, String> tags = new MultivaluedHashMap<String, String>();
        tags.add("thread", "current");
        GetSeriesQuery seriesQuery = new GetSeriesQuery(SSS_ENTITY, SSS_METRIC);
        seriesQuery.setStartTime(System.currentTimeMillis() - 10000);
        seriesQuery.setEndTime(System.currentTimeMillis() + 1000);
        seriesQuery.setTags(tags);
        GetSeriesQuery seriesQuery2 = new GetSeriesQuery(SSS_ENTITY, YYY_METRIC);
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

        private final String tagValue;
        private int cnt = 1;
        private long sleep = 0;

        public SimpleSeriesSender(long startMs, DataService dataService, CountDownLatch latch, String tagValue) {
            this.startMs = startMs;
            this.dataService = dataService;
            this.latch = latch;
            this.tagValue = tagValue;
        }

        @Override
        public void run() {
            Series series = new Series(startMs + counter.incrementAndGet(), Math.random());
            AbstractInsertCommand plainCommand = new InsertCommand(SSS_ENTITY, SSS_METRIC, series,
                    SSS_TAG, tagValue);
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

    private int countSssSeries(long start, MultivaluedHashMap<String, String> tags) {
        GetSeriesQuery seriesQuery = new GetSeriesQuery(SSS_ENTITY, SSS_METRIC);
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




    public GetSeriesQuery createTestGetTestCommand() {
        MultivaluedHashMap<String, String> tags = new MultivaluedHashMap<String, String>();
        tags.add("ttt-tag-1", "ttt-tag-value-1");
        tags.add("ttt-tag-2", "ttt-tag-value-2");
        GetSeriesQuery command = new GetSeriesQuery(TTT_ENTITY, TTT_METRIC);
        command.setTags(tags);
        command.setAggregateMatcher(new SimpleAggregateMatcher(new Interval(20, IntervalUnit.SECOND),
                Interpolate.NONE,
                AggregateType.DETAIL));
        command.setInterval(new Interval(20, IntervalUnit.DAY));
//        command.setStartTime(System.currentTimeMillis() - 100000000L);
//        command.setEndTime(System.currentTimeMillis() + 100000000L);

        return command;
    }


}