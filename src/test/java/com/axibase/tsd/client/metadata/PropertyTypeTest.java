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
import com.axibase.tsd.model.data.Property;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.axibase.tsd.TestUtil.*;
import static junit.framework.Assert.*;

/**
 * @author Dmitry Korchagin.
 */
public class PropertyTypeTest {
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
    public void testRetrievePropertyTypes() throws Exception {
        final String typeNameFirst = buildVariablePrefix() + "type-first";
        final String typeNameSecond = buildVariablePrefix() + "type-second";
        final String entityName = buildVariablePrefix() + "entity";
        final Long timestart = 0L;
        Map<String, String> key = new HashMap<>();
        key.put("key1", "key1-val");
        key.put("key2", "key2-val");
        Map<String, String> tags = new HashMap<>();
        tags.put("tag-key", "tag-value");

        if (metaDataService.retrievePropertyTypes(entityName, timestart).size() < 2) {
            assertTrue(dataService.insertProperties(
                            new Property(typeNameFirst, entityName, key, tags),
                            new Property(typeNameSecond, entityName, key, tags)
                    )
            );
        }

        Set propertyTypes = metaDataService.retrievePropertyTypes(entityName, timestart);
        assertNotNull(propertyTypes);
        assertEquals(2, propertyTypes.size());
        assertTrue(propertyTypes.contains(typeNameFirst));
        assertTrue(propertyTypes.contains(typeNameSecond));
    }

}
