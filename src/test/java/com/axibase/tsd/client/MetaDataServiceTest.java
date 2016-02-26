/*
 * Copyright 2016 Axibase Corporation or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * https://www.axibase.com/atsd/axibase-apache-2.0.pdf
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.axibase.tsd.client;

import com.axibase.tsd.RerunRule;
import com.axibase.tsd.TestUtil;
import com.axibase.tsd.model.data.Property;
import com.axibase.tsd.model.data.command.AddSeriesCommand;
import com.axibase.tsd.model.data.series.Series;
import com.axibase.tsd.model.meta.*;
import org.junit.*;

import java.util.*;

import static com.axibase.tsd.TestUtil.*;
import static junit.framework.Assert.*;
import static junit.framework.Assert.assertEquals;

/**
 * @author Nikolay Malevanny.
 */
public class MetaDataServiceTest {
    private MetaDataService metaDataService;
    private DataService dataService;
    private HttpClientManager httpClientManager;

    @Rule
    public RerunRule rerunRule = new RerunRule();

    @Before
    public void setUp() throws Exception {
        httpClientManager = TestUtil.buildHttpClientManager();
        metaDataService = new MetaDataService();
        metaDataService.setHttpClientManager(httpClientManager);
        dataService = new DataService();
        dataService.setHttpClientManager(httpClientManager);

        waitWorkingServer(httpClientManager);
    }

    @Test
    public void testRetrieveMetrics() throws Exception {
        List metrics = metaDataService.retrieveMetrics(true, "name like '*'", TagAppender.ALL, 1);
        assertTrue(metrics.get(0) instanceof Metric);
        assertEquals(1, metrics.size());
    }

    @Test
    public void testRetrieveMetric() throws Exception {
        final String metricName = "test-retrive-metric";
        if(metaDataService.retrieveMetric(metricName) == null) {
            assertTrue(metaDataService.createOrReplaceMetric(createNewTestMetric(metricName)));
        }

        Metric metric = metaDataService.retrieveMetric(metricName);
        assertNotNull(metric);
        assertEquals(metric.getName(), metricName);
    }

    @Test
    public void testCreateOrReplaceMetric() throws Exception {
        final String metricName = "test-create-or-replace-metric";
        if(metaDataService.retrieveMetric(metricName) != null) {
            assertTrue(metaDataService.createOrReplaceMetric(createNewTestMetric(metricName)));
        }

        Metric metric = createNewTestMetric(metricName);
        {
            metric.setDataType(DataType.DOUBLE);
            Assert.assertTrue(metaDataService.createOrReplaceMetric(metric));
            metric = metaDataService.retrieveMetric(metricName);
            assertEquals(metric.getName(),metricName);
            assertEquals(metric.getDataType(), DataType.DOUBLE);
        }

        {
            metric.setDataType(DataType.FLOAT);
            Assert.assertTrue(metaDataService.createOrReplaceMetric(metric));
            metric = metaDataService.retrieveMetric(metricName);
            assertEquals(metric.getName(), metricName);
            assertEquals(metric.getDataType(), DataType.FLOAT);
        }
    }

    @Test
    public void testCreateAndDeleteMetric() throws Exception {
        final String metricName = "test-create-and-delete-metric";
        Metric metric = createNewTestMetric(metricName);

        if(metaDataService.retrieveMetric(metricName) != null) {
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
        final String metricName = "test-update-metric";
        if(metaDataService.retrieveMetric(metricName) != null) {
            assertTrue(metaDataService.deleteMetric(createNewTestMetric(metricName)));//TODO why we send Metric, but use only metricName?
        }
        assertNull(metaDataService.retrieveMetric(metricName));

        Metric metric = createNewTestMetric(metricName);
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

    @Test
    public void testRetrieveEntity() throws Exception {
        final String entityName = "test-retrieve-entity";
        if(metaDataService.retrieveEntity(entityName) == null) {
            assertTrue(metaDataService.createOrReplaceEntity(new Entity(entityName)));
        }
        assertEquals(entityName, metaDataService.retrieveEntity(entityName).getName());
    }

    @Test
    public void testRetrieveEntities() throws Exception {
        final String entityName = "test-retrieve-entities";
        if(metaDataService.retrieveEntity(entityName) == null) {
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
    public void testCreateOrReplaceEntityWithoutTags() throws  Exception {
        final String entityName = "test-create-or-replace-entity-without-tags";
        if (metaDataService.retrieveEntity(entityName) != null) {
            metaDataService.deleteEntity(new Entity(entityName));
        }
        assertNull(metaDataService.retrieveEntity(entityName));

        Assert.assertTrue(metaDataService.createOrReplaceEntity(new Entity(entityName)));
        Entity newEntity = metaDataService.retrieveEntity(entityName);
        assertEquals(entityName, newEntity.getName());
        assertEquals(new HashMap<>(), newEntity.getTags());
    }

    @Test
    public void testCreateOrReplaceEntityWithTags() throws Exception {
        final String entityName = "test-create-or-replace-entity-with-tags";
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
            Assert.assertTrue(metaDataService.createOrReplaceEntity(entity));
            entity = metaDataService.retrieveEntity(entityName);
            assertEquals(entityName, entity.getName());
            assertEquals(entity.getTags(), tags);
        }

        {
            Map<String, String> tags = new HashMap<>();
            tags.put("test-tag2", "test-tag2-val2");
            tags.put("test-tag3", "test-tag3-val");
            entity.setTags(tags);
            Assert.assertTrue(metaDataService.createOrReplaceEntity(entity));
            entity = metaDataService.retrieveEntity(entityName);
            assertEquals(entityName, entity.getName());
            assertEquals(entity.getTags(), tags);
        }
    }

    @Ignore
    @Test
    public void testCreateOrReplaceInvalidEntityWithoutTags() throws  Exception {
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
    @Ignore
    public void testCreateOrReplaceEntityWithInvalidTags() throws Exception {
        final String entityName = "test-create-entity-with-invalid-tags";

        if (metaDataService.retrieveEntity(entityName) != null) {
            metaDataService.deleteEntity(new Entity(entityName));
        }
        assertNull(metaDataService.retrieveEntity(entityName));

        Entity entity = new Entity(entityName);
        entity.buildTags("test- t__\\\'\" onclick=alert(1) 'g1", "test-__-  tag1-val", "test-tag2", "test-tag2-val");
        Assert.assertFalse(metaDataService.createOrReplaceEntity(entity));
        assertNull(metaDataService.retrieveEntity(entityName));
    }

    @Test
    public void testCreateAndDeleteEntity() throws Exception {
        final String entityName = "test-create-and-delete-entity";
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

    @Test
    public void testRetrieveEntityGroups() throws Exception {
        List<EntityGroup> entityGroups = metaDataService.retrieveEntityGroups();
        assertTrue(entityGroups.get(0) instanceof EntityGroup);
        assertTrue(entityGroups.size() > 0);
        boolean containsTestEntityGroup = false;
        for (Iterator<EntityGroup> iterator = entityGroups.iterator(); iterator.hasNext() && !containsTestEntityGroup; ) {
            EntityGroup entityGroup = iterator.next();
            containsTestEntityGroup = TTT_ENTITY_GROUP.equals(entityGroup.getName());
        }
        assertTrue(containsTestEntityGroup);
    }

    @Test
    public void testRetrieveEntityGroup() throws Exception {
        EntityGroup entityGroup = metaDataService.retrieveEntityGroup(TTT_ENTITY_GROUP);
        assertEquals(TTT_ENTITY_GROUP, entityGroup.getName());
    }

    @Test
    public void testCreateOrReplaceEntityGroup() throws Exception {
        EntityGroup entityGroup = metaDataService.retrieveEntityGroup(TTT_ENTITY_GROUP);
        assertEquals(TTT_ENTITY_GROUP, entityGroup.getName());
        if (entityGroup.getTags().containsKey("uuu-tag-1")) {
            entityGroup.getTags().remove("uuu-tag-1");
            metaDataService.createOrReplaceEntityGroup(entityGroup);
        }

        Map<String, String> tags = entityGroup.getTags();
        Map<String, String> savedTags = new HashMap<String, String>(tags);
        tags.put("uuu-tag-1", "uuu-tag-value-1");
        entityGroup.setTags(tags);
        assertTrue(metaDataService.createOrReplaceEntityGroup(entityGroup));

        EntityGroup updatedEntityGroup = metaDataService.retrieveEntityGroup(TTT_ENTITY_GROUP);
        assertEquals(TTT_ENTITY_GROUP, updatedEntityGroup.getName());
        assertTrue(updatedEntityGroup.getTags().containsKey("uuu-tag-1"));

        updatedEntityGroup.setTags(savedTags);
        metaDataService.createOrReplaceEntityGroup(updatedEntityGroup);

        entityGroup = metaDataService.retrieveEntityGroup(TTT_ENTITY_GROUP);
        assertEquals(TTT_ENTITY_GROUP, entityGroup.getName());
        assertFalse(entityGroup.getTags().containsKey("uuu-tag-1"));
    }

    @Test
    public void testCreateAndDeleteEntityGroup() throws Exception {
        EntityGroup nullGroup = metaDataService.retrieveEntityGroup(NNN_ENTITY_GROUP);
        if (nullGroup != null) {
            metaDataService.deleteEntityGroup(nullGroup);
        }

        EntityGroup entityGroup = new EntityGroup(NNN_ENTITY_GROUP);
        entityGroup.setTags("nnn-test-tag-1", "nnn-test-tag-value-1");
        metaDataService.createOrReplaceEntityGroup(entityGroup);

        assertNotNull(metaDataService.retrieveEntityGroup(NNN_ENTITY_GROUP));

        metaDataService.deleteEntityGroup(entityGroup);

        assertNull(metaDataService.retrieveEntityGroup(NNN_ENTITY_GROUP));
    }

    @Test
    public void testUpdateEntityGroup() throws Exception {
        EntityGroup entityGroup = metaDataService.retrieveEntityGroup(TTT_ENTITY_GROUP);
        Map<String, String> oldTags = entityGroup.getTags();
        if (oldTags.containsKey(UUU_TAG)) {
            oldTags.remove(UUU_TAG);
            metaDataService.createOrReplaceEntityGroup(entityGroup);
        }

        Map<String, String> newTags = new HashMap<>(oldTags);
        newTags.put(UUU_TAG, "uuu-tag-value");
        entityGroup.setTags(newTags);

        metaDataService.updateEntityGroup(entityGroup);

        entityGroup = metaDataService.retrieveEntityGroup(TTT_ENTITY_GROUP);
        assertTrue(entityGroup.getTags().containsKey(UUU_TAG));

        entityGroup.setTags(oldTags);
        metaDataService.createOrReplaceEntityGroup(entityGroup);

        entityGroup = metaDataService.retrieveEntityGroup(TTT_ENTITY_GROUP);
        assertFalse(entityGroup.getTags().containsKey(UUU_TAG));
    }

    @Test
    public void testRetrieveGroupEntities() throws Exception {
        assertTrue(metaDataService.deleteGroupEntities(TTT_ENTITY_GROUP
                , new Entity("java-uuu-entity")
                , new Entity("java-sss-entity")));
        List<Entity> entityList = metaDataService
                .retrieveGroupEntities(TTT_ENTITY_GROUP, null, null, TagAppender.ALL, null);
        if (entityList.size() == 0) {
            entityList = fixTestDataEntityGroupEntity();
        }
        Entity entity = entityList.get(0);
        assertEquals(1, entityList.size());
        assertEquals(TTT_ENTITY, entity.getName());
    }

    @Test
    public void testManageGroupEntities() throws Exception {
        {
            List<Entity> entityList = metaDataService
                    .retrieveGroupEntities(TTT_ENTITY_GROUP, null, null, TagAppender.ALL, null);
            if (entityList.size() == 0) {
                entityList = fixTestDataEntityGroupEntity();
            }
            Entity entity = entityList.get(0);
            assertEquals(1, entityList.size());
            assertEquals(TTT_ENTITY, entity.getName());
        }
        assertEquals(1, metaDataService.retrieveGroupEntities(TTT_ENTITY_GROUP).size());
        assertTrue(metaDataService.addGroupEntities(TTT_ENTITY_GROUP, true, new Entity("java-uuu-entity")));
        assertEquals(2, metaDataService.retrieveGroupEntities(TTT_ENTITY_GROUP).size());
        assertTrue(metaDataService.replaceGroupEntities(TTT_ENTITY_GROUP, true, new Entity(TTT_ENTITY), new Entity("java-sss-entity")));
        assertTrue(metaDataService.deleteGroupEntities(TTT_ENTITY_GROUP
                , new Entity("java-sss-entity")));
        assertEquals(1, metaDataService.retrieveGroupEntities(TTT_ENTITY_GROUP).size());
    }

    @Test
    public void testRetrieveEntityAndTagsByMetric() throws Exception {
        final String entityName = "test-retrive-entity-and-tags-by-metric-entity";
        final String metricName = "test-retrive-entity-and-tags-by-metric-metric";
        Map<String, String> tags = new HashMap<>();
        tags.put("test-tag1", "test-tag1-val");
        tags.put("test-tag2", "test-tag2-val");

        if(metaDataService.retrieveEntity(entityName) == null) {
            AddSeriesCommand addSeriesCommand = new AddSeriesCommand(entityName, metricName, "test-tag1", "test-tag1-val", "test-tag2", "test-tag2-val");
            addSeriesCommand.addSeries(new Series(1456489150000L, 1));
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
        final String entityName = "test-retrive-entity-and-tags-by-metric-and-entity-entity";
        final String metricName = "test-retrive-entity-and-tags-by-metric-and-entity-metric";
        Map<String, String> tags = new HashMap<>();
        tags.put("test-tag1", "test-tag1-val");
        tags.put("test-tag2", "test-tag2-val");

        if(metaDataService.retrieveEntity(entityName) == null) {
            AddSeriesCommand addSeriesCommand = new AddSeriesCommand(entityName, metricName, "test-tag1", "test-tag1-val", "test-tag2", "test-tag2-val");
            addSeriesCommand.addSeries(new Series(1456489150000L, 1));
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

    @Test
    public void testRetrievePropertyTypes() throws Exception {
        List<Property> properties = dataService.retrieveProperties(buildPropertiesQuery());
        if (properties.size() < 2) {
            fixTestDataProperty(dataService);
        }
        Set<String> propertyTypes = metaDataService.retrievePropertyTypes(TTT_ENTITY, 0L);
        assertTrue(propertyTypes.size() > 0);
        assertTrue(propertyTypes.contains(TTT_TYPE));
        assertTrue(propertyTypes.contains(TTT_TYPE + ".t"));
    }


    @Test
    public void testRetrieveMetricsByEntity() throws Exception {
        final String metricName = "test-retrieve-metrics-by-entity-metric";
        final String entityName = "test-retrieve-metrics-by-entity-entity";
        if(metaDataService.retrieveMetric(metricName) == null) {
            AddSeriesCommand addSeriesCommand = new AddSeriesCommand(entityName, metricName, "test-tag1", "test-tag1-val", "test-tag2", "test-tag2-val");
            addSeriesCommand.addSeries(new Series(1456489150000L, 1));
            assertTrue(dataService.addSeries(addSeriesCommand));
        }
        List metrics = metaDataService.retrieveMetrics(entityName, null, "name like '*'", null, 1);
        assertEquals(1, metrics.size());
        assertTrue(metrics.get(0) instanceof Metric);
        assertEquals(((Metric) metrics.get(0)).getName(), metricName);
    }

    @After
    public void tearDown() {
        httpClientManager.close();
    }

    private Metric createNewTestMetric(String metricName) {
        Metric newMetric = new Metric();
        newMetric.setName(metricName);
        newMetric.setDataType(DataType.INTEGER);
        newMetric.setDescription("test");
        newMetric.setEnabled(false);
        newMetric.setMaxValue(1D);
        newMetric.setMinValue(3D);
        newMetric.buildTags(
                "nnn-tag-1", "nnn-tag-value-1",
                "nnn-tag-2", "nnn-tag-value-2"
        );
        newMetric.setTimePrecision(TimePrecision.SECONDS);
        return newMetric;
    }

    private List<Entity> fixTestDataEntityGroupEntity() {
        List<Entity> entityList;
        metaDataService.addGroupEntities(TTT_ENTITY_GROUP, true, new Entity(TTT_ENTITY));
        entityList = metaDataService
                .retrieveGroupEntities(TTT_ENTITY_GROUP, null, null, TagAppender.ALL, null);
        return entityList;
    }
}
