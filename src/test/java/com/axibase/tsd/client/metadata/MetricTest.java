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

package com.axibase.tsd.client.metadata;

import com.axibase.tsd.RerunRule;
import com.axibase.tsd.TestUtil;
import com.axibase.tsd.client.DataService;
import com.axibase.tsd.client.HttpClientManager;
import com.axibase.tsd.client.MetaDataService;
import com.axibase.tsd.model.data.command.AddSeriesCommand;
import com.axibase.tsd.model.data.series.Sample;
import com.axibase.tsd.model.data.series.Series;
import com.axibase.tsd.model.meta.DataType;
import com.axibase.tsd.model.meta.Metric;
import com.axibase.tsd.model.meta.TagAppender;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.axibase.tsd.TestUtil.*;
import static junit.framework.Assert.*;


public class MetricTest {
    private MetaDataService metaDataService;
    private DataService dataService;
    private HttpClientManager httpClientManager;

    @Rule
    public RerunRule rerunRule = new RerunRule();

    @Before
    public void setUp() throws Exception {
        httpClientManager = buildHttpClientManager();
        metaDataService = new MetaDataService();
        metaDataService.setHttpClientManager(httpClientManager);
        dataService = new DataService();
        dataService.setHttpClientManager(httpClientManager);

        waitWorkingServer(httpClientManager);
    }

    @After
    public void tearDown() {
        httpClientManager.close();
    }

    @Test
    public void testRetrieveMetricsByPattern() throws Exception {
        final String metricQuery = "name like '*'";
        final String metricName = buildVariablePrefix();
        if (metaDataService.retrieveMetrics(true, "name like '*'", TagAppender.ALL, 1).size() < 1) {
            assertTrue(metaDataService.createOrReplaceMetric(createNewTestMetric(metricName)));
        }

        List metrics = metaDataService.retrieveMetrics(true, metricQuery, TagAppender.ALL, 1);
        assertTrue(metrics.get(0) instanceof Metric);
        assertEquals(1, metrics.size());

        metrics = metaDataService.retrieveMetrics(metricQuery, (String) null, null, TagAppender.ALL, 1);
        assertTrue(metrics.get(0) instanceof Metric);
        assertEquals(1, metrics.size());
    }

    @Test
    public void testRetrieveMetricByName() throws Exception {
        final String metricName = buildVariablePrefix();
        if (metaDataService.retrieveMetric(metricName) == null) {
            assertTrue(metaDataService.createOrReplaceMetric(createNewTestMetric(metricName)));
        }

        Metric metric = metaDataService.retrieveMetric(metricName);
        assertNotNull(metric);
        assertEquals(metric.getName(), metricName);
    }

    @Test
    public void testCreateOrReplaceMetric() throws Exception {
        final String metricName = buildVariablePrefix();
        if (metaDataService.retrieveMetric(metricName) != null) {
            assertTrue(metaDataService.createOrReplaceMetric(createNewTestMetric(metricName)));
        }

        Metric metric = createNewTestMetric(metricName);
        {
            metric.setDataType(DataType.DOUBLE);
            assertTrue(metaDataService.createOrReplaceMetric(metric));
            metric = metaDataService.retrieveMetric(metricName);
            assertEquals(metric.getName(), metricName);
            assertEquals(metric.getDataType(), DataType.DOUBLE);
        }

        {
            metric.setDataType(DataType.FLOAT);
            assertTrue(metaDataService.createOrReplaceMetric(metric));
            metric = metaDataService.retrieveMetric(metricName);
            assertEquals(metric.getName(), metricName);
            assertEquals(metric.getDataType(), DataType.FLOAT);
        }
    }

    @Test
    public void testCreateAndDeleteMetric() throws Exception {
        final String metricName = buildVariablePrefix();
        Metric metric = createNewTestMetric(metricName);

        if (metaDataService.retrieveMetric(metricName) != null) {
            assertTrue(metaDataService.deleteMetric(metric));
        }
        assertNull(metaDataService.retrieveMetric(metricName));

        assertTrue(metaDataService.createOrReplaceMetric(metric));

        Metric insertedMetric = metaDataService.retrieveMetric(metricName);
        assertNotNull(insertedMetric);
        assertEquals(insertedMetric.getName(), metric.getName());
        assertEquals(insertedMetric.getTags(), metric.getTags());

        assertTrue(metaDataService.deleteMetric(insertedMetric));
        assertNull(metaDataService.retrieveMetric(metricName));
    }

    @Test
    public void testUpdateMetric() throws Exception {
        final String metricName = buildVariablePrefix();
        if (metaDataService.retrieveMetric(metricName) != null) {
            assertTrue(metaDataService.deleteMetric(createNewTestMetric(metricName)));
        }
        assertNull(metaDataService.retrieveMetric(metricName));

        Metric metric = createNewTestMetric(metricName);

        {
            Map<String, String> defaultTags = new HashMap<>();
            defaultTags.put("test-tag1", "test-tag1-val");
            defaultTags.put("test-tag2", "test-tag2-val");
            metric.setTags(defaultTags);
            assertTrue(metaDataService.createOrReplaceMetric(metric));


            metric = metaDataService.retrieveMetric(metricName);
            assertNotNull(metric);
            assertEquals(metric.getName(), metricName);
            assertEquals(metric.getTags(), defaultTags);
            assertEquals(metric.getTags().get("test-tag2"), "test-tag2-val");
        }

        {
            Map<String, String> newTags = new HashMap<>();
            newTags.put("test-tag2", "test-tag2-new-val");
            newTags.put("test-tag3", "test-tag3-val");
            newTags.put("test-tag4", "test-tag4-val");

            metric.setTags(newTags);

            assertTrue(metaDataService.updateMetric(metric));

            metric = metaDataService.retrieveMetric(metricName);
            assertNotNull(metric);
            assertTrue(metric.getTags().containsKey("test-tag1"));
            assertTrue(metric.getTags().containsKey("test-tag2"));
            assertTrue(metric.getTags().containsKey("test-tag3"));
            assertTrue(metric.getTags().containsKey("test-tag4"));
            assertEquals(metric.getTags().get("test-tag2"), "test-tag2-new-val");
        }


    }

    @Test
    public void testRetrieveMetricsByEntity() throws Exception {
        final String metricName = buildVariablePrefix() + "metric";
        final String entityName = buildVariablePrefix() + "entity";
        final Long timestamp = MOCK_TIMESTAMP;
        if (metaDataService.retrieveMetrics(entityName, (Boolean) null, "name like '*'", null, 1).isEmpty()) {
            AddSeriesCommand addSeriesCommand = new AddSeriesCommand(entityName, metricName, "test-tag1", "test-tag1-val", "test-tag2", "test-tag2-val");
            addSeriesCommand.addSeries(new Sample(timestamp, 1));
            assertTrue(dataService.addSeries(addSeriesCommand));
        }
        List metrics = metaDataService.retrieveMetrics(entityName, (Boolean) null, "name like '*'", null, 1);
        assertEquals(1, metrics.size());
        assertTrue(metrics.get(0) instanceof Metric);
        assertEquals(((Metric) metrics.get(0)).getName(), metricName);

        metrics = metaDataService.retrieveMetrics(entityName, "name like '*'", (String) null, null, null, null, 1);
        assertEquals(1, metrics.size());
        assertTrue(metrics.get(0) instanceof Metric);
        assertEquals(((Metric) metrics.get(0)).getName(), metricName);
    }


    @Test
    public void testRetrieveMetricSeries() {
        String testPrefix = TestUtil.buildVariablePrefix();
        Series series = new Series();
        series.setMetricName(testPrefix + "-metric");
        series.setEntityName(testPrefix + "-entity");
        series.setData(Collections.singletonList(new Sample(MOCK_TIMESTAMP, MOCK_SERIE_VALUE)));
        AddSeriesCommand command = new AddSeriesCommand(series.getEntityName(), series.getMetricName(), null);
        command.addSeries(series.getData());
        dataService.addSeries(command);
        TestUtil.waitWorkingServer(httpClientManager);

        List<Series> seriesList = metaDataService.retrieveMetricSeries(series.getMetricName());

        String assertMessage = String.format(
                "Incorrect series list for metric %s",
                series.getMetricName()
        );
        assertEquals("Incorrect count of series", 1, seriesList.size());
        assertEquals(assertMessage, series.getMetricName(), seriesList.get(0).getMetricName());
    }
}
