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
import com.axibase.tsd.model.data.Alert;
import com.axibase.tsd.model.data.Severity;
import com.axibase.tsd.model.data.TimeFormat;
import com.axibase.tsd.model.data.command.BatchAlertCommand;
import com.axibase.tsd.model.data.command.GetAlertQuery;
import com.axibase.tsd.network.PlainCommand;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.axibase.tsd.TestUtil.*;
import static junit.framework.Assert.*;

public class AlertTest {

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

    @After
    public void tearDown() throws Exception {
        httpClientManager.close();
    }


    @Test
    public void testRetrieveAlerts() throws Exception {
        PlainCommand plainCommand = createFireAlertSeriesCommand();
        // fire alert
        dataService.sendPlainCommand(plainCommand);
        System.out.println("command = " + plainCommand.compose());
        Thread.sleep(WAIT_TIME);
        {
            List<String> metrics = Arrays.asList(TTT_METRIC);
            List<String> entities = Arrays.asList(TTT_ENTITY);
            List<Alert> alerts = dataService.retrieveAlerts(metrics, entities, null, null, null, TimeFormat.MILLISECONDS);
            assertNotNull(alerts);
        }
        {
            {
                List<Alert> alerts = dataService.retrieveAlerts(null, Collections.singletonList("ttt-entity"), null, null, null, TimeFormat.MILLISECONDS);
                assertNotNull(alerts);
                assertTrue(alerts.size() > 0);
            }

            List<Alert> alerts;
            {
                alerts = dataService.retrieveAlerts(null, Collections.singletonList("ttt-entity"), null, null, null, TimeFormat.ISO);
                assertNotNull(alerts);
                assertTrue(alerts.size() > 0);
                Alert alert = alerts.get(0);
                assertTrue(StringUtils.isNoneBlank(alert.getOpenDate()));
            }

            // clean
            String[] ids = toIds(alerts);
            dataService.batchUpdateAlerts(BatchAlertCommand.createUpdateCommand(true, ids));
        }
    }


    private String[] toIds(List<Alert> alerts) {
        String[] ids = new String[alerts.size()];
        for (int i = 0; i < alerts.size(); i++) {
            ids[i] = "" + alerts.get(i).getId();
        }
        return ids;
    }


    @Test
    public void testUpdateAlerts() throws Exception {
        GetAlertQuery query = new GetAlertQuery(
                Arrays.asList(TTT_METRIC),
                Arrays.asList(TTT_ENTITY),
                Arrays.asList(TTT_RULE),
                Collections.<Integer>emptyList(),
                Severity.UNKNOWN.getId(),
                TimeFormat.MILLISECONDS
        );
        query.setStartTime(0L);
        query.setEndTime(System.currentTimeMillis() + 10000);

        { // clean
            List<Alert> alerts = dataService.retrieveAlerts(query);
            String[] ids = toIds(alerts);
            if (ids.length > 0) {
                dataService.batchUpdateAlerts(BatchAlertCommand.createDeleteCommand(ids));
            }
        }

        // fire alert
        dataService.sendPlainCommand(createFireAlertSeriesCommand());
        Thread.sleep(WAIT_TIME);


        // check alert
        List<Alert> alerts = dataService.retrieveAlerts(query);
        assertTrue(alerts.size() > 0);
        Alert alert = alerts.get(0);
        assertFalse(alert.getAcknowledged());

        // update alerts
        String[] ids = toIds(alerts);
        dataService.batchUpdateAlerts(BatchAlertCommand.createUpdateCommand(true, ids));

        Thread.sleep(WAIT_TIME);

        // check updated alert
        alerts = dataService.retrieveAlerts(query);
        assertTrue(alerts.get(0).getAcknowledged());

        // delete alerts
        dataService.batchUpdateAlerts(BatchAlertCommand.createDeleteCommand(ids));

        // check empty
        assertTrue(dataService.retrieveAlerts(query).isEmpty());
    }


    private PlainCommand createFireAlertSeriesCommand() {
        return new PlainCommand() {
            @Override
            public String compose() {
                return "series e:ttt-entity t:ttt-tag-1=ttt-tag-value-1 m:ttt-metric=35791.0";
            }
        };
    }





}