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
import com.axibase.tsd.model.meta.Entity;
import com.axibase.tsd.model.meta.EntityGroup;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.axibase.tsd.TestUtil.*;
import static junit.framework.Assert.*;

/**
 * @author Dmitry Korchagin.
 */
public class EntityGroupTest {
    private MetaDataService metaDataService;
    private HttpClientManager httpClientManager;

    @Rule
    public RerunRule rerunRule = new RerunRule();

    @Before
    public void setUp() throws Exception {
        httpClientManager = buildHttpClientManager();
        metaDataService = new MetaDataService();
        metaDataService.setHttpClientManager(httpClientManager);
        DataService dataService = new DataService();
        dataService.setHttpClientManager(httpClientManager);

        waitWorkingServer(httpClientManager);
    }

    @After
    public void tearDown() {
        httpClientManager.close();
    }

    @Test
    public void testRetrieveAllEntityGroups() throws Exception {
        final String entityGroupName = buildVariablePrefix();
        if (metaDataService.retrieveEntityGroup(entityGroupName) == null) {
            assertTrue(metaDataService.createOrReplaceEntityGroup(new EntityGroup(entityGroupName)));
        }
        List<EntityGroup> entityGroups = metaDataService.retrieveEntityGroups();
        assertTrue(entityGroups.size() > 0);
        assertTrue(entityGroups.get(0) instanceof EntityGroup);

        boolean containsTestEntityGroup = false;
        for (Iterator<EntityGroup> iterator = entityGroups.iterator(); iterator.hasNext() && !containsTestEntityGroup; ) {
            EntityGroup entityGroup = iterator.next();
            containsTestEntityGroup = entityGroupName.equals(entityGroup.getName());
        }
        assertTrue(containsTestEntityGroup);
    }

    @Test
    public void testRetrieveEntityGroupByName() throws Exception {
        final String entityGroupName = buildVariablePrefix();
        if (metaDataService.retrieveEntityGroup(entityGroupName) == null) {
            assertTrue(metaDataService.createOrReplaceEntityGroup(new EntityGroup(entityGroupName)));
        }
        EntityGroup entityGroup = metaDataService.retrieveEntityGroup(entityGroupName);
        assertNotNull(entityGroup);
        assertEquals(entityGroup.getName(), entityGroupName);
    }

    @Test
    public void testCreateOrReplaceEntityGroupWithTags() throws Exception {
        final String entityGroupName = buildVariablePrefix();
        if (metaDataService.retrieveEntityGroup(entityGroupName) != null) {
            metaDataService.deleteEntityGroup(new EntityGroup(entityGroupName));
        }
        assertNull(metaDataService.retrieveEntityGroup(entityGroupName));

        EntityGroup entityGroup = new EntityGroup(entityGroupName);

        {
            Map<String, String> tags = new HashMap<>();
            tags.put("test-tag1", "test-tag1-val");
            tags.put("test-tag2", "test-tag2-val1");
            entityGroup.setTags(tags);
            assertTrue(metaDataService.createOrReplaceEntityGroup(entityGroup));
            entityGroup = metaDataService.retrieveEntityGroup(entityGroupName);
            assertEquals(entityGroup.getName(), entityGroupName);
            assertEquals(entityGroup.getTags(), tags);
        }

        {
            Map<String, String> tags = new HashMap<>();
            tags.put("test-tag2", "test-tag2-val2");
            tags.put("test-tag3", "test-tag3-val");
            entityGroup.setTags(tags);
            assertTrue(metaDataService.createOrReplaceEntityGroup(entityGroup));
            entityGroup = metaDataService.retrieveEntityGroup(entityGroupName);
            assertEquals(entityGroup.getName(), entityGroupName);
            assertEquals(entityGroup.getTags(), tags);
        }
    }

    @Test
    public void testCreateAndDeleteEntityGroupWithoutTags() throws Exception {
        final String entityGroupName = buildVariablePrefix();
        if (metaDataService.retrieveEntityGroup(entityGroupName) != null) {
            assertTrue(metaDataService.deleteEntityGroup(new EntityGroup(entityGroupName)));
        }
        assertNull(metaDataService.retrieveEntityGroup(entityGroupName));

        assertTrue(metaDataService.createOrReplaceEntityGroup(new EntityGroup(entityGroupName)));

        EntityGroup entityGroup = metaDataService.retrieveEntityGroup(entityGroupName);
        assertNotNull(entityGroup);
        assertEquals(entityGroup.getName(), entityGroupName);

        assertTrue(metaDataService.deleteEntityGroup(entityGroup));
        assertNull(metaDataService.retrieveEntityGroup(entityGroupName));
    }

    @Test
    public void testUpdateEntityGroup() throws Exception {
        final String entityGroupName = buildVariablePrefix();
        if (metaDataService.retrieveMetric(entityGroupName) != null) {
            assertTrue(metaDataService.deleteMetric(createNewTestMetric(entityGroupName)));
        }
        assertNull(metaDataService.retrieveMetric(entityGroupName));

        EntityGroup entityGroup = new EntityGroup(entityGroupName);

        {
            Map<String, String> defaultTags = new HashMap<>();
            defaultTags.put("test-tag1", "test-tag1-val");
            defaultTags.put("test-tag2", "test-tag2-val");
            entityGroup.setTags(defaultTags);
            assertTrue(metaDataService.createOrReplaceEntityGroup(entityGroup));


            entityGroup = metaDataService.retrieveEntityGroup(entityGroupName);
            assertNotNull(entityGroup);
            assertEquals(entityGroup.getName(), entityGroupName);
            assertEquals(entityGroup.getTags(), defaultTags);
            assertEquals(entityGroup.getTags().get("test-tag2"), "test-tag2-val");
        }

        {
            Map<String, String> newTags = new HashMap<>();
            newTags.put("test-tag2", "test-tag2-new-val");
            newTags.put("test-tag3", "test-tag3-val");
            newTags.put("test-tag4", "test-tag4-val");

            entityGroup.setTags(newTags);

            assertTrue(metaDataService.updateEntityGroup(entityGroup));

            entityGroup = metaDataService.retrieveEntityGroup(entityGroupName);
            assertNotNull(entityGroup);
            assertTrue(entityGroup.getTags().containsKey("test-tag1"));
            assertTrue(entityGroup.getTags().containsKey("test-tag2"));
            assertTrue(entityGroup.getTags().containsKey("test-tag3"));
            assertTrue(entityGroup.getTags().containsKey("test-tag4"));
            assertEquals(entityGroup.getTags().get("test-tag2"), "test-tag2-new-val");
        }
    }

    @Test
    public void testRetrieveGroupEntitiesByGroupName() throws Exception {
        final String entityGroupName = buildVariablePrefix() + "group";
        final String entityNameFrist = buildVariablePrefix() + "first";
        final String entityNameSeconds = buildVariablePrefix() + "second";
        List<Entity> entities = metaDataService.retrieveGroupEntities(entityGroupName);
        if (entities.size() < 2) {
            EntityGroup entityGroup = new EntityGroup(entityGroupName);
            assertTrue(metaDataService.createOrReplaceEntityGroup(entityGroup));
            assertTrue(metaDataService.addGroupEntities(entityGroupName, true, new Entity(entityNameFrist), new Entity(entityNameSeconds)));
        }
        entities = metaDataService.retrieveGroupEntities(entityGroupName);
        assertEquals(2, entities.size());
        assertTrue(entities.get(0) instanceof Entity);
        assertTrue((entityNameFrist.concat(entityNameSeconds)).contains(entities.get(0).getName()));
        assertTrue((entityNameFrist.concat(entityNameSeconds)).contains(entities.get(1).getName()));
    }

    @Test
    public void testManageGroupEntities() throws Exception {
        final String entityGroupName = buildVariablePrefix() + "group";
        if (metaDataService.retrieveEntityGroup(entityGroupName) == null) {
            assertTrue(metaDataService.createOrReplaceEntityGroup(new EntityGroup(entityGroupName)));
        }
        assertNotNull(metaDataService.retrieveEntityGroup(entityGroupName));
        if (!metaDataService.retrieveGroupEntities(entityGroupName).isEmpty()) {
            assertTrue(metaDataService.deleteAllGroupEntities(entityGroupName));
        }
        assertTrue(metaDataService.retrieveGroupEntities(entityGroupName).isEmpty());

        {
            final String oldEntityName = buildVariablePrefix() + "old-entity";
            assertTrue(metaDataService.addGroupEntities(entityGroupName, true, new Entity(oldEntityName)));
            List entity = metaDataService.retrieveGroupEntities(entityGroupName);
            assertEquals(1, entity.size());
            assertTrue(entity.get(0) instanceof Entity);
            assertEquals(oldEntityName, ((Entity) entity.get(0)).getName());
        }


        final String newEntityNameFirst = buildVariablePrefix() + "new-entity-first";
        final String newEntityNameSecond = buildVariablePrefix() + "new-entity-second";
        {
            assertTrue(metaDataService.replaceGroupEntities(entityGroupName, true, new Entity(newEntityNameFirst), new Entity(newEntityNameSecond)));
            List entity = metaDataService.retrieveGroupEntities(entityGroupName);
            assertEquals(2, entity.size());
            assertTrue(entity.get(0) instanceof Entity);
            assertTrue(newEntityNameFirst.concat(newEntityNameSecond).contains(((Entity) entity.get(0)).getName()));
            assertTrue(newEntityNameFirst.concat(newEntityNameSecond).contains(((Entity) entity.get(1)).getName()));
        }

        {
            assertTrue(metaDataService.deleteGroupEntities(entityGroupName, new Entity(newEntityNameFirst)));
            List entity = metaDataService.retrieveGroupEntities(entityGroupName);
            assertEquals(1, entity.size());
            assertTrue(entity.get(0) instanceof Entity);
            assertEquals(newEntityNameSecond, ((Entity) entity.get(0)).getName());
        }

        {
            assertTrue(metaDataService.deleteAllGroupEntities(entityGroupName));
            List entity = metaDataService.retrieveGroupEntities(entityGroupName);
            assertTrue(entity.isEmpty());
        }
    }


}
