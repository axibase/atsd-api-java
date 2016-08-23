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
package com.axibase.tsd.client.data;

import com.axibase.tsd.RerunRule;
import com.axibase.tsd.client.AtsdServerException;
import com.axibase.tsd.client.DataService;
import com.axibase.tsd.client.HttpClientManager;
import com.axibase.tsd.model.data.Property;
import com.axibase.tsd.model.data.command.GetPropertiesQuery;
import com.axibase.tsd.model.data.filters.DeletePropertyFilter;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.axibase.tsd.TestUtil.*;
import static junit.framework.Assert.*;

public class PropertyTest {
    private Logger logger = LoggerFactory.getLogger(Property.class);
    private DataService dataService;
    private HttpClientManager httpClientManager;

    @Rule
    public RerunRule rerunRule = new RerunRule();

    @Before
    public void setUp() throws Exception {
        httpClientManager = buildHttpClientManager();
        httpClientManager.setCheckPeriodMillis(1000);
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
        final String entityName = buildVariablePrefix() + "entity";
        final String propertyTypeName = buildVariablePrefix() + "type";
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
        assertFalse(dataService.retrieveProperties(entityName, propertyTypeName).isEmpty());

        GetPropertiesQuery getPropertiesQuery = new GetPropertiesQuery(propertyTypeName, entityName)
                .setKey(key);

        List<Property> properties = dataService.retrieveProperties(getPropertiesQuery);
        assertFalse(properties.isEmpty());
        logger.info(properties.toString());
        assertEquals(entityName, properties.get(0).getEntityName());
        assertEquals(propertyTypeName, properties.get(0).getType());
        assertEquals(key, properties.get(0).getKey());
    }


    @Test
    public void testRetrievePropertiesByEntityNameAndPropertyTypeName() throws Exception {
        final String entityName = buildVariablePrefix() + "entity";
        final String propertyTypeName = buildVariablePrefix() + "property-type";
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
        assertFalse(properties.isEmpty());
        assertTrue(properties.get(0) instanceof Property);
        assertEquals(1, properties.size());


        assertEquals(entityName, ((Property) properties.get(0)).getEntityName());
        assertEquals(propertyTypeName, ((Property) properties.get(0)).getType());
    }

    @Test
    public void testInsertProperties() throws Exception {
        final String propertyTypeName = buildVariablePrefix() + "type";
        final String entityName = buildVariablePrefix() + "entity";
        Map<String, String> key = new HashMap<>();
        key.put("key1", "key1-val");
        key.put("key2", "key2-val");
        Map<String, String> tags = new HashMap<>();
        tags.put("tag1", "tag1-val");
        tags.put("tag2", "tag2-val");
        Property property = new Property(propertyTypeName, entityName, key, tags);

        if (!dataService.retrieveProperties(entityName, propertyTypeName).isEmpty()) {
            assertTrue(dataService.deleteProperties(new DeletePropertyFilter(propertyTypeName, entityName)));
        }
        assertTrue(dataService.retrieveProperties(entityName, propertyTypeName).isEmpty());

        property.setTags(tags);
        assertTrue(dataService.insertProperties(property));

        List properties = dataService.retrieveProperties(entityName, propertyTypeName);

        assertFalse(properties.isEmpty());
        assertEquals(1, properties.size());
        assertTrue(properties.get(0) instanceof Property);
        logger.info("debug start");
        logger.info(properties.toString());
        logger.info("debug end");
        assertEquals(key, ((Property) properties.get(0)).getKey());
        assertEquals(tags, ((Property) properties.get(0)).getTags());
    }

    @Test
    public void testInsertPropertiesWithoutTags() throws Exception {
        final String propertyTypeName = buildVariablePrefix() + "typeName";
        final String entityName = buildVariablePrefix() + "entity";
        Map<String, String> key = new HashMap<>();
        key.put("key1", "key1-val");
        key.put("key2", "key2-val");
        Property property = new Property(propertyTypeName, entityName, key, null);

        if (!dataService.retrieveProperties(entityName, propertyTypeName).isEmpty()) {
            assertTrue(dataService.deleteProperties(new DeletePropertyFilter(propertyTypeName, entityName)));
        }
        assertTrue(dataService.retrieveProperties(entityName, propertyTypeName).isEmpty());
        assertFalse(dataService.insertProperties(property));
        assertTrue(dataService.retrieveProperties(entityName, propertyTypeName).isEmpty());
    }

    @Test(expected = AtsdServerException.class)
    public void testInsertPropertiesWithoutEntity() throws Exception {
        final String propertyTypeName = buildVariablePrefix() + "typeName";
        Map<String, String> key = new HashMap<>();
        key.put("key1", "key1-val");
        key.put("key2", "key2-val");
        Map<String, String> tags = new HashMap<>();
        tags.put("tag1", "tag1-val");
        tags.put("tag2", "tag2-val");
        Property property = new Property(propertyTypeName, null, key, tags);
        if (!dataService.retrieveProperties(new GetPropertiesQuery(propertyTypeName, null)).isEmpty()) {
            assertTrue(dataService.deleteProperties(new DeletePropertyFilter(propertyTypeName, null)));
        }
        assertTrue(dataService.retrieveProperties(new GetPropertiesQuery(propertyTypeName, null)).isEmpty());
        try {
            dataService.insertProperties(property);
            fail();
        } catch (IllegalArgumentException e) {
            //ok
        }
        assertTrue(dataService.retrieveProperties(new GetPropertiesQuery(propertyTypeName, null)).isEmpty());
    }

    @Test(expected = AtsdServerException.class)
    public void testInsertPropertiesWithoutPropertyType() throws Exception {
        final String entityName = buildVariablePrefix() + "entityName";
        Map<String, String> key = new HashMap<>();
        key.put("key1", "key1-val");
        key.put("key2", "key2-val");
        Map<String, String> tags = new HashMap<>();
        tags.put("tag1", "tag1-val");
        tags.put("tag2", "tag2-val");
        Property property = new Property(null, entityName, key, tags);

        List<Property> oldProperties = dataService.retrieveProperties(new GetPropertiesQuery(null, entityName));
        if (!oldProperties.isEmpty()) {
            List<DeletePropertyFilter> batchPropertyCommands = new ArrayList<>();
            for (Property prop : oldProperties) {
                batchPropertyCommands.add(new DeletePropertyFilter(prop.getType(), prop.getEntityName()));
            }
            assertTrue(dataService.deleteProperties(batchPropertyCommands));
        }
        assertTrue(dataService.retrieveProperties(new GetPropertiesQuery(null, entityName)).isEmpty());
        try {
            dataService.insertProperties(property);
            fail();
        } catch (IllegalArgumentException e) {
            //ok
        }
        assertTrue(dataService.retrieveProperties(new GetPropertiesQuery(null, entityName)).isEmpty());
    }


    @Test
    public void testBatchDeletePropertiesNoTime() throws Exception {
        final String propertyTypeName = buildVariablePrefix() + "property-type";
        final String entityName = buildVariablePrefix() + "entity";
        final Map<String, String> key = new HashMap<>();
        key.put("key1", "key1-val");
        key.put("key2", "key2-val");
        Map<String, String> tags = new HashMap<>();
        tags.put("tag1", "tag1-val");
        tags.put("tag2", "tag2-val");
        Property property = new Property(propertyTypeName, entityName, key, tags);


        if (dataService.retrieveProperties(entityName, propertyTypeName).isEmpty()) {
            assertTrue(dataService.insertProperties(property));
        }
        assertFalse(dataService.retrieveProperties(entityName, propertyTypeName).isEmpty());

        DeletePropertyFilter filter = new DeletePropertyFilter(propertyTypeName, entityName);
        filter.setKey(key);
        assertTrue(dataService.deleteProperties(filter));
        assertTrue(dataService.retrieveProperties(entityName, propertyTypeName).isEmpty());
    }

    @Test
    public void testBatchDeletePropertiesTimestamp() throws Exception {
        final String propertyTypeName = buildVariablePrefix() + "property-type";
        final String entityName = buildVariablePrefix() + "entity";
        final Map<String, String> key = new HashMap<>();
        key.put("key1", "key1-val");
        key.put("key2", "key2-val");
        Map<String, String> tags = new HashMap<>();
        tags.put("tag1", "tag1-val");
        tags.put("tag2", "tag2-val");
        Property property = new Property(propertyTypeName, entityName, key, tags);


        if (dataService.retrieveProperties(entityName, propertyTypeName).isEmpty()) {
            assertTrue(dataService.insertProperties(property));
        }
        assertFalse(dataService.retrieveProperties(entityName, propertyTypeName).isEmpty());

        DeletePropertyFilter filter = new DeletePropertyFilter(propertyTypeName, entityName);
        filter.setKey(key);

        assertTrue(dataService.deleteProperties(filter));
        assertTrue(dataService.retrieveProperties(entityName, propertyTypeName).isEmpty());
    }

}