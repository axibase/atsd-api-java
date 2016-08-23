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
import com.axibase.tsd.client.DataService;
import com.axibase.tsd.client.HttpClientManager;
import com.axibase.tsd.client.MetaDataService;
import com.axibase.tsd.model.data.command.AddSeriesCommand;
import com.axibase.tsd.model.data.series.Sample;
import com.axibase.tsd.model.meta.EntityAndTags;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.axibase.tsd.TestUtil.*;
import static junit.framework.Assert.*;

/**
 * @author Dmitry Korchagin.
 */
public class EntityAndTagsTest {
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
    public void testRetrieveEntityAndTagsByMetric() throws Exception {
        final String entityName = buildVariablePrefix() + "entity";
        final String metricName = buildVariablePrefix() + "metric";
        final Long timestamp = MOCK_TIMESTAMP;
        Map<String, String> tags = new HashMap<>();
        tags.put("test-tag1", "test-tag1-val");
        tags.put("test-tag2", "test-tag2-val");

        if (metaDataService.retrieveEntity(entityName) == null) {
            AddSeriesCommand addSeriesCommand = new AddSeriesCommand(entityName, metricName, "test-tag1", "test-tag1-val", "test-tag2", "test-tag2-val");
            addSeriesCommand.addSeries(new Sample(timestamp, 1));
            assertTrue(dataService.addSeries(addSeriesCommand));
        }

        List entityAndTagsList = metaDataService.retrieveEntityAndTags(metricName, null);

        assertTrue(entityAndTagsList.size() > 0);
        assertTrue(entityAndTagsList.get(0) instanceof EntityAndTags);
        assertEquals(((EntityAndTags) entityAndTagsList.get(0)).getEntityName(), entityName);
        assertEquals(((EntityAndTags) entityAndTagsList.get(0)).getTags(), tags);


    }

    @Test
    public void testRetrieveEntityAndTagsByMetricAndEntity() throws Exception {
        final String entityName = buildVariablePrefix() + "entity";
        final String metricName = buildVariablePrefix() + "metric";
        final Long timestamp = MOCK_TIMESTAMP;

        Map<String, String> tags = new HashMap<>();
        tags.put("test-tag1", "test-tag1-val");
        tags.put("test-tag2", "test-tag2-val");

        if (metaDataService.retrieveEntity(entityName) == null) {
            AddSeriesCommand addSeriesCommand = new AddSeriesCommand(entityName, metricName, "test-tag1", "test-tag1-val", "test-tag2", "test-tag2-val");
            addSeriesCommand.addSeries(new Sample(timestamp, 1));
            assertTrue(dataService.addSeries(addSeriesCommand));
        }

        List entityAndTagsList = metaDataService.retrieveEntityAndTags(metricName, entityName);

        assertTrue(entityAndTagsList.size() > 0);
        assertTrue(entityAndTagsList.get(0) instanceof EntityAndTags);
        assertEquals(((EntityAndTags) entityAndTagsList.get(0)).getEntityName(), entityName);

        try {
            metaDataService.retrieveEntityAndTags(" ", " ");
            fail();
        } catch (IllegalArgumentException e) {
            // OK
        }
    }
}
