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
import com.axibase.tsd.model.data.command.BatchAlertCommand;
import com.axibase.tsd.model.data.command.GetAlertQuery;
import com.axibase.tsd.network.PlainCommand;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
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
        final String entityName = buildVariablePrefix();
        final String metricName = ALERTS_METRIC;
        final Long timestamp = System.currentTimeMillis(); //required to activate alerts

        PlainCommand plainCommand = insertSeriesCommandToCreateAlert(entityName, metricName, timestamp);
        // fire alert
        dataService.sendPlainCommand(plainCommand);
        System.out.println("command = " + plainCommand.compose());
        Thread.sleep(WAIT_TIME);
        GetAlertQuery getAlertQuery = new GetAlertQuery();
        getAlertQuery.setEntityNames(Arrays.asList(entityName));
        getAlertQuery.setMetricNames(Arrays.asList(metricName));
        List alerts = dataService.retrieveAlerts(getAlertQuery);
        assertNotNull(alerts);
        assertFalse(alerts.isEmpty());
        assertTrue(alerts.get(0) instanceof Alert);

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
        final String entityName = buildVariablePrefix();
        final String metricName = ALERTS_METRIC;
        final Long timestamp = System.currentTimeMillis(); //required to activate alerts

        GetAlertQuery getAlertQuery = new GetAlertQuery();
        getAlertQuery.setEntityNames(Arrays.asList(entityName));
        getAlertQuery.setMetricNames(Arrays.asList(metricName));

        { // clean
            List<Alert> alerts = dataService.retrieveAlerts(getAlertQuery);
            String[] ids = toIds(alerts);
            if (ids.length > 0) {
                dataService.batchUpdateAlerts(BatchAlertCommand.createDeleteCommand(ids));
            }
        }
        assertTrue(dataService.retrieveAlerts(getAlertQuery).isEmpty());

        // fire alert
        PlainCommand plainCommand = insertSeriesCommandToCreateAlert(entityName, metricName, timestamp);
        dataService.sendPlainCommand(plainCommand);
        Thread.sleep(WAIT_TIME);


        // check alert
        List<Alert> alerts = dataService.retrieveAlerts(getAlertQuery);
        assertTrue(alerts.size() > 0);
        Alert alert = alerts.get(0);
        assertFalse(alert.getAcknowledged());

        // update alerts
        String[] ids = toIds(alerts);
        dataService.batchUpdateAlerts(BatchAlertCommand.createUpdateCommand(true, ids));

        Thread.sleep(WAIT_TIME);

        // check updated alert
        alerts = dataService.retrieveAlerts(getAlertQuery);
        assertTrue(alerts.get(0).getAcknowledged());

        // delete alerts
        assertTrue(dataService.batchUpdateAlerts(BatchAlertCommand.createDeleteCommand(ids)));

        // check empty
        assertTrue(dataService.retrieveAlerts(getAlertQuery).isEmpty());
    }


    private PlainCommand insertSeriesCommandToCreateAlert(final String entityName, final String metricName, final Long timestamp) {
        return new PlainCommand() {
            @Override
            public String compose() {
                StringBuilder sb = new StringBuilder();
                sb
                        .append("series e:")
                        .append(entityName)
                        .append(" m:")
                        .append(metricName)
                        .append("=1 ms:")
                        .append(timestamp);
                return sb.toString();
            }
        };
    }


}