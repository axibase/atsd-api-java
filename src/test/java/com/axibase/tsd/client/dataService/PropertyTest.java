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
import org.junit.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.axibase.tsd.TestUtil.buildVariablePrefix;
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
        assertTrue(!dataService.retrieveProperties(entityName, propertyTypeName).isEmpty());

        GetPropertiesQuery getPropertiesQuery = new GetPropertiesQuery(propertyTypeName, entityName).setKey(key);

        List properties = dataService.retrieveProperties(getPropertiesQuery);
        assertFalse(properties.isEmpty());
        assertTrue(properties.get(0) instanceof Property);
        System.out.println(properties);
        assertEquals(entityName, ((Property) properties.get(0)).getEntityName());
        assertEquals(propertyTypeName, ((Property) properties.get(0)).getType());
        assertEquals(key, ((Property) properties.get(0)).getKey());
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
        assertTrue(!properties.isEmpty());
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
            assertTrue(dataService.batchUpdateProperties(BatchPropertyCommand.createDeleteCommand(new PropertyMatcher(propertyTypeName))));
        }
        assertTrue(dataService.retrieveProperties(entityName, propertyTypeName).isEmpty());

        property.setTags(tags);
        assertTrue(dataService.insertProperties(property));

        List properties = dataService.retrieveProperties(entityName, propertyTypeName);

        assertFalse(properties.isEmpty());
        assertEquals(1, properties.size());
        assertTrue(properties.get(0) instanceof Property);
        System.out.println("debug start");
        System.out.println(properties);
        System.out.println("debug end");
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
            assertTrue(dataService.batchUpdateProperties(BatchPropertyCommand.createDeleteCommand(new PropertyMatcher(propertyTypeName))));
        }
        assertTrue(dataService.retrieveProperties(entityName, propertyTypeName).isEmpty());
        assertFalse(dataService.insertProperties(property));
        assertTrue(dataService.retrieveProperties(entityName, propertyTypeName).isEmpty());
    }

    @Test
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
            assertTrue(dataService.batchUpdateProperties(BatchPropertyCommand.createDeleteCommand(new PropertyMatcher(propertyTypeName))));
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

    @Test
    public void testInsertPropertiesWithoutPropertyType() throws Exception {
        final String entityName = buildVariablePrefix() + "entityName";
        Map<String, String> key = new HashMap<>();
        key.put("key1", "key1-val");
        key.put("key2", "key2-val");
        Map<String, String> tags = new HashMap<>();
        tags.put("tag1", "tag1-val");
        tags.put("tag2", "tag2-val");
        Property property = new Property(null, entityName, key, tags);

        List<Property> oldProperties =  dataService.retrieveProperties(new GetPropertiesQuery(null, entityName));
        if (!oldProperties.isEmpty()) {
            List<BatchPropertyCommand> batchPropertyCommands = new ArrayList<>();
            for(Property prop: oldProperties) {
                batchPropertyCommands.add(BatchPropertyCommand.createDeleteCommand(new PropertyMatcher(prop.getType(), prop.getEntityName())));
            }
            assertTrue(dataService.batchUpdateProperties(batchPropertyCommands.toArray(new BatchPropertyCommand[batchPropertyCommands.size()])));
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

        PropertyMatcher propertyMatcher = new PropertyMatcher(propertyTypeName, entityName, key);

        assertTrue(dataService.batchUpdateProperties(BatchPropertyCommand.createDeleteCommand(propertyMatcher)));
        assertTrue(dataService.retrieveProperties(entityName, propertyTypeName).isEmpty());
    }

    @Test
    public void testBatchDeletePropertiesTimestamp() throws Exception {
        final String propertyTypeName = buildVariablePrefix() + "property-type";
        final String entityName = buildVariablePrefix() + "entity";
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

        PropertyMatcher propertyMatcher = new PropertyMatcher(propertyTypeName, entityName, key);
        propertyMatcher.setCreatedBeforeTime(Long.MAX_VALUE);

        assertTrue(dataService.batchUpdateProperties(BatchPropertyCommand.createDeleteCommand(propertyMatcher)));
        assertTrue(dataService.retrieveProperties(entityName, propertyTypeName).isEmpty());
    }

    @Test
    public void testBatchDeletePropertiesDate() throws Exception {
        final String propertyTypeName = buildVariablePrefix() + "property-type";
        final String entityName = buildVariablePrefix() + "entity";
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

        PropertyMatcher propertyMatcher = new PropertyMatcher(propertyTypeName, entityName, key);

        propertyMatcher.setCreatedBeforeDate("3016-03-25T00:00:00.000Z");

        assertTrue(dataService.batchUpdateProperties(BatchPropertyCommand.createDeleteCommand(propertyMatcher)));
        assertTrue(dataService.retrieveProperties(entityName, propertyTypeName).isEmpty());
    }



    @Ignore //not available to test it by client now
    @Test
    public void testBatchDeletePropertiesByPropertyWithTags() throws Exception {
        final String propertyTypeName = buildVariablePrefix() + "property-type";
        final String entityName = buildVariablePrefix() + "entity";
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



//        assertFalse(dataService.batchUpdateProperties(BatchPropertyCommand.createDeleteCommand(property)));
        assertFalse(dataService.retrieveProperties(entityName, propertyTypeName).isEmpty());
    }


//    @Test
//    public void testDeletePropertiesByPropertyWithoutTags() throws Exception {
//        final String propertyTypeName = buildVariablePrefix() + "property-type";
//        final String entityName = buildVariablePrefix() + "entity";
//        Map<String, String> key = new HashMap<>();
//        key.put("key1", "key1-val");
//        key.put("key2", "key2-val");
//        Map<String, String> tags = new HashMap<>();
//        tags.put("tag1", "tag1-val");
//        tags.put("tag2", "tag2-val");
//        Property property = new Property(propertyTypeName, entityName, key, tags);
//
//
//        if (dataService.retrieveProperties(entityName, propertyTypeName).isEmpty()) {
//            assertTrue(dataService.insertProperties(property));
//        }
//        assertFalse(dataService.retrieveProperties(entityName, propertyTypeName).isEmpty());
//
//        property.setTags(null);
//        assertTrue(dataService.batchUpdateProperties(BatchPropertyCommand.createDeleteCommand(property)));
//        assertTrue(dataService.retrieveProperties(entityName, propertyTypeName).isEmpty());
//    }


}