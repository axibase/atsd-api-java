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
 */
package com.axibase.tsd.client.dataService;

import com.axibase.tsd.RerunRule;
import com.axibase.tsd.TestUtil;
import com.axibase.tsd.client.DataService;
import com.axibase.tsd.client.HttpClientManager;
import com.axibase.tsd.model.data.AlertHistory;
import com.axibase.tsd.model.data.command.GetAlertHistoryQuery;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;

import static com.axibase.tsd.TestUtil.*;
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
    public void testRetrieveAlertHistory() throws Exception {
        GetAlertHistoryQuery getAlertHistoryQuery = new GetAlertHistoryQuery();
        getAlertHistoryQuery.setStartTime(0L);
        getAlertHistoryQuery.setEndTime(Long.MAX_VALUE);
        getAlertHistoryQuery.setEntityName(TTT_ENTITY);
        getAlertHistoryQuery.setMetricName(TTT_METRIC);

        List<AlertHistory> alertHistoryList = dataService.retrieveAlertHistory(getAlertHistoryQuery);
        assertTrue(alertHistoryList.get(0) instanceof AlertHistory);
        assertTrue(alertHistoryList.size() > 0);
    }


    @After
    public void tearDown() throws Exception {
        httpClientManager.close();
    }

}