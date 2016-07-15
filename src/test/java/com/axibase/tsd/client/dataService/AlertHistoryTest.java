/*
 *
 *  * Copyright 2016 Axibase Corporation or its affiliates. All Rights Reserved.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License").
 *  * You may not use this file except in compliance with the License.
 *  * A copy of the License is located at
 *  *
 *  * https://www.axibase.com/atsd/axibase-apache-2.0.pdf
 *  *
 *  * or in the "license" file accompanying this file. This file is distributed
 *  * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  * express or implied. See the License for the specific language governing
 *  * permissions and limitations under the License.
 *
 *//*

package com.axibase.tsd.client.dataService;

import com.axibase.tsd.RerunRule;
import com.axibase.tsd.TestUtil;
import com.axibase.tsd.client.DataService;
import com.axibase.tsd.client.HttpClientManager;
import com.axibase.tsd.model.data.AlertHistory;
import com.axibase.tsd.model.data.command.AddSeriesCommand;
import com.axibase.tsd.model.data.command.GetAlertHistoryQuery;
import com.axibase.tsd.model.data.series.Sample;
import org.junit.*;

import java.util.List;

import static com.axibase.tsd.TestUtil.*;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;

public class AlertHistoryTest {

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
    public void testRetrieveAlertHistoryByMetricAndTimes() throws Exception {
        final String entityName = buildVariablePrefix();
        final String metricName = ALERTS_METRIC;
        final Long timestamp = System.currentTimeMillis(); //required to activate alerts
        final Long delta = 50L;
        GetAlertHistoryQuery getAlertHistoryQuery = new GetAlertHistoryQuery()
                .setMetricName(metricName)
                .setStartTime(timestamp - delta)
                .setEndTime(timestamp + delta);
        if (dataService.retrieveAlertHistory(getAlertHistoryQuery).isEmpty()) {
            AddSeriesCommand addSeriesCommand = new AddSeriesCommand(entityName, metricName);
            addSeriesCommand.addSeries(new Sample(timestamp, 1));
            dataService.addSeries(addSeriesCommand);
            addSeriesCommand = new AddSeriesCommand(entityName, metricName);
            addSeriesCommand.addSeries(new Sample(timestamp+1, -1));
            dataService.addSeries(addSeriesCommand);
        }
        List alertHistoryList = dataService.retrieveAlertHistory(getAlertHistoryQuery);
        assertFalse(alertHistoryList.isEmpty());
        assertTrue(alertHistoryList.get(0) instanceof AlertHistory);
    }


    @Test
    public void testRetrieveAlertHistoryByMetric() throws Exception {
        final String entityName = buildVariablePrefix();
        final String metricName = ALERTS_METRIC;
        final Long timestamp = System.currentTimeMillis(); //required to activate alerts
        GetAlertHistoryQuery getAlertHistoryQuery = new GetAlertHistoryQuery()
                .setMetricName(metricName);
        if (dataService.retrieveAlertHistory(getAlertHistoryQuery).isEmpty()) {
            AddSeriesCommand addSeriesCommand = new AddSeriesCommand(entityName, metricName);
            addSeriesCommand.addSeries(new Sample(timestamp, 1));
            dataService.addSeries(addSeriesCommand);
            addSeriesCommand = new AddSeriesCommand(entityName, metricName);
            addSeriesCommand.addSeries(new Sample(timestamp+1, -1));
            dataService.addSeries(addSeriesCommand);
        }
        List alertHistoryList = dataService.retrieveAlertHistory(getAlertHistoryQuery);
        assertFalse(alertHistoryList.isEmpty());
        assertTrue(alertHistoryList.get(0) instanceof AlertHistory);
    }


    @After
    public void tearDown() throws Exception {
        httpClientManager.close();
    }

}*/
