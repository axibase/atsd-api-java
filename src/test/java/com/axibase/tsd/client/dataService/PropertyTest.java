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
import com.axibase.tsd.model.data.Property;
import com.axibase.tsd.model.data.command.BatchPropertyCommand;
import com.axibase.tsd.model.data.command.GetPropertiesQuery;
import com.axibase.tsd.model.data.command.PropertyMatcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.axibase.tsd.TestUtil.getVariablePrefix;
import static com.axibase.tsd.TestUtil.waitWorkingServer;
import static junit.framework.Assert.*;

public class PropertyTest {

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
    public void testRetrievePropertiesByPropertiesQuery() throws Exception {
        final String entityName = getVariablePrefix() + "entity";
        final String propertyTypeName = getVariablePrefix() + "type";
        Map<String, String> key = new HashMap<>();
        key.put("key1", "key1-val");
        key.put("key2", "key2-val");
        Map<String, String> tags = new HashMap<>();
        tags.put("tag1", "tag1-val");
        tags.put("tag2", "tag2-val");

        Property property = new Property(propertyTypeName, entityName, key, tags);

        if (dataService.retrieveProperties(entityName, propertyTypeName).isEmpty()) {
            assertTrue(dataService.insertProperties(property));
        }
        assertTrue(!dataService.retrieveProperties(entityName, propertyTypeName).isEmpty());

        GetPropertiesQuery getPropertiesQuery = new GetPropertiesQuery(propertyTypeName, entityName).setKey(key);

        List properties = dataService.retrieveProperties(getPropertiesQuery);
        assertFalse(properties.isEmpty());
        assertTrue(properties.get(0) instanceof Property);

        assertEquals(entityName, ((Property) properties.get(0)).getEntityName());
        assertEquals(propertyTypeName, ((Property) properties.get(0)).getType());
        assertEquals(key, ((Property) properties.get(0)).getKey());
    }


//    @Test
//    public void testRetrievePropertiesWithDate() throws Exception {
//        getPropertiesQuery.setTimeFormat(TimeFormat.ISO);
//        List<Property> properties = dataService.retrieveProperties(getPropertiesQuery);
//        if (properties.size() == 0) {
//        }
//
//        for (Property property : properties) {
//            System.out.println("property = " + property);
//        }
//        Property property = properties.get(0);
//        assertEquals(1, properties.size());
//        assertTrue(StringUtils.isNoneBlank(property.getDate()));
//    }

    @Test
    public void testRetrievePropertiesByEntityNameAndPropertyTypeName() throws Exception {
        final String entityName = getVariablePrefix() + "entity";
        final String propertyTypeName = getVariablePrefix() + "property-type";
        Map<String, String> key = new HashMap<>();
        key.put("key1", "key1-val");
        key.put("key2", "key2-val");
        Map<String, String> tags = new HashMap<>();
        tags.put("tag1", "tag1-val");
        tags.put("tag2", "tag2-val");

        if (dataService.retrieveProperties(entityName, propertyTypeName).isEmpty()) {
            assertTrue(dataService.insertProperties(new Property(propertyTypeName, entityName, key, tags)));
        }

        List properties = dataService.retrieveProperties(entityName, propertyTypeName);
        assertTrue(!properties.isEmpty());
        assertTrue(properties.get(0) instanceof Property);
        assertEquals(1, properties.size());
        assertEquals(entityName, ((Property) properties.get(0)).getEntityName());
        assertEquals(propertyTypeName, ((Property) properties.get(0)).getType());
    }

    @Test
    public void testInsertProperties() throws Exception {
        final String propertyTypeName = getVariablePrefix() + "type";
        final String entityName = getVariablePrefix() + "entity";
        Map<String, String> key = new HashMap<>();
        key.put("key1", "key1-val");
        key.put("key2", "key2-val");
        Map<String, String> tags = new HashMap<>();
        tags.put("tag1", "tag1-val");
        tags.put("tag2", "tag2-val");
        Property property = new Property(propertyTypeName, entityName, key, tags);

        if (!dataService.retrieveProperties(entityName, propertyTypeName).isEmpty()) {
            assertTrue(dataService.batchUpdateProperties(BatchPropertyCommand.createDeleteCommand(property)));
        }
        assertTrue(dataService.retrieveProperties(entityName, propertyTypeName).isEmpty());

        assertTrue(dataService.insertProperties(property));

        List properties = dataService.retrieveProperties(entityName, propertyTypeName);
        assertTrue(!properties.isEmpty());
        assertEquals(1, properties.size());
        assertTrue(properties.get(0) instanceof Property);
        assertEquals(key, ((Property) properties.get(0)).getKey());
        assertEquals(tags, ((Property) properties.get(0)).getTags());
    }


    @Test
    public void testDeletePropertiesByMatcher() throws Exception {
        final String propertyTypeName = getVariablePrefix() + "property-type";
        final String entityName = getVariablePrefix() + "entity";
        Map<String, String> key = new HashMap<>();
        key.put("key1", "key1-val");
        key.put("key2", "key2-val");
        Map<String, String> tags = new HashMap<>();
        tags.put("tag1", "tag1-val");
        tags.put("tag2", "tag2-val");
        Property property = new Property(propertyTypeName, entityName, key, tags);


        if (dataService.retrieveProperties(entityName, propertyTypeName).isEmpty()) {
            assertTrue(dataService.insertProperties(property));
        }
        assertTrue(!dataService.retrieveProperties(entityName, propertyTypeName).isEmpty());

        PropertyMatcher propertyMatcher = new PropertyMatcher(propertyTypeName, entityName, Long.MAX_VALUE).setKey(key);

        assertTrue(dataService.batchUpdateProperties(BatchPropertyCommand.createDeleteMatchCommand(propertyMatcher)));
        assertTrue(dataService.retrieveProperties(entityName, propertyTypeName).isEmpty());
    }


}