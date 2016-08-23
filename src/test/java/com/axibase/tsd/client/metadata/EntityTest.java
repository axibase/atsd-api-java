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
import com.axibase.tsd.client.AtsdServerException;
import com.axibase.tsd.client.DataService;
import com.axibase.tsd.client.HttpClientManager;
import com.axibase.tsd.client.MetaDataService;
import com.axibase.tsd.model.meta.Entity;
import com.axibase.tsd.model.meta.TagAppender;
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
public class EntityTest {
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
    public void testRetrieveEntity() throws Exception {
        final String entityName = buildVariablePrefix();
        if (metaDataService.retrieveEntity(entityName) == null) {
            assertTrue(metaDataService.createOrReplaceEntity(new Entity(entityName)));
        }
        assertEquals(entityName, metaDataService.retrieveEntity(entityName).getName());
    }

    @Test
    public void testRetrieveEntities() throws Exception {
        final String entityName = buildVariablePrefix();
        if (metaDataService.retrieveEntity(entityName) == null) {
            assertTrue(metaDataService.createOrReplaceEntity(new Entity(entityName)));
        }

        {
            List<Entity> entities = metaDataService.retrieveEntities(null, "name like '*'", TagAppender.ALL, 1);
            assertEquals(1, entities.size());
            assertTrue(entities.get(0) instanceof Entity);
        }

        {
            List entities = metaDataService.retrieveEntities(null, "name = '" + entityName + "'", TagAppender.ALL, 1);
            assertEquals(1, entities.size());

            assertTrue(entities.get(0) instanceof Entity);
            assertEquals(((Entity) entities.get(0)).getName(), entityName);
        }
    }

    @Test
    public void testCreateOrReplaceEntityWithoutTags() throws Exception {
        final String entityName = buildVariablePrefix();
        if (metaDataService.retrieveEntity(entityName) != null) {
            metaDataService.deleteEntity(new Entity(entityName));
        }
        assertNull(metaDataService.retrieveEntity(entityName));

        assertTrue(metaDataService.createOrReplaceEntity(new Entity(entityName)));
        Entity newEntity = metaDataService.retrieveEntity(entityName);
        assertEquals(entityName, newEntity.getName());
        assertEquals(new HashMap<>(), newEntity.getTags());
    }

    @Test
    public void testCreateOrReplaceEntityWithTags() throws Exception {
        final String entityName = buildVariablePrefix();
        if (metaDataService.retrieveEntity(entityName) != null) {
            metaDataService.deleteEntity(new Entity(entityName));
        }
        assertNull(metaDataService.retrieveEntity(entityName));

        Entity entity = new Entity(entityName);

        {
            Map<String, String> tags = new HashMap<>();
            tags.put("test-tag1", "test-tag1-val");
            tags.put("test-tag2", "test-tag2-val1");
            entity.setTags(tags);
            assertTrue(metaDataService.createOrReplaceEntity(entity));
            entity = metaDataService.retrieveEntity(entityName);
            assertEquals(entity.getName(), entityName);
            assertEquals(entity.getTags(), tags);
        }

        {
            Map<String, String> tags = new HashMap<>();
            tags.put("test-tag2", "test-tag2-val2");
            tags.put("test-tag3", "test-tag3-val");
            entity.setTags(tags);
            assertTrue(metaDataService.createOrReplaceEntity(entity));
            entity = metaDataService.retrieveEntity(entityName);
            assertEquals(entity.getName(), entityName);
            assertEquals(entity.getTags(), tags);
        }
    }

    @Test(expected = AtsdServerException.class)
    public void testCreateOrReplaceInvalidEntityWithoutTags() throws Exception {
        final String entityName = "te_____st-cre ate-invalid-^%entityƒџќѕ∆-w\"ith''ou't-tags";

        if (metaDataService.retrieveEntity(entityName) != null) {
            metaDataService.deleteEntity(new Entity(entityName));
        }
        assertNull(metaDataService.retrieveEntity(entityName));

        Entity entity = new Entity(entityName);
        assertFalse(metaDataService.createOrReplaceEntity(entity));
        assertNull(metaDataService.retrieveEntity(entityName));
    }

    @Test
    public void testCreateOrReplaceEntityWithInvalidTags() throws Exception {
        final String entityName = buildVariablePrefix();

        if (metaDataService.retrieveEntity(entityName) != null) {
            metaDataService.deleteEntity(new Entity(entityName));
        }
        assertNull(metaDataService.retrieveEntity(entityName));
        Entity entity = new Entity(entityName);
        entity.buildTags("test- t__\\\'\" onclick=alert(1) 'g1", "test-__-  tag1-val", "test-tag2", "test-tag2-val");
        assertFalse(metaDataService.createOrReplaceEntity(entity));
        assertNull(metaDataService.retrieveEntity(entityName));
    }

    @Test
    public void testCreateAndDeleteEntity() throws Exception {
        final String entityName = buildVariablePrefix();
        if (metaDataService.retrieveEntity(entityName) != null) {
            metaDataService.deleteEntity(new Entity(entityName));
        }
        assertNull(metaDataService.retrieveEntity(entityName));

        Entity entity = new Entity(entityName);
        entity.buildTags("nnn-test-tag-1", "nnn-test-tag-value-1");
        assertTrue(metaDataService.createOrReplaceEntity(entity));

        Entity newEntity = metaDataService.retrieveEntity(entityName);
        assertNotNull(newEntity);
        assertEquals(newEntity.getName(), entityName);
        assertEquals(newEntity.getTags(), entity.getTags());

        assertTrue(metaDataService.deleteEntity(entity));
        assertNull(metaDataService.retrieveEntity(entityName));
    }

}
