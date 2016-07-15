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

import com.axibase.tsd.model.meta.*;
import com.axibase.tsd.model.meta.command.AddEntitiesCommand;
import com.axibase.tsd.model.meta.command.DeleteEntitiesCommand;
import com.axibase.tsd.model.meta.command.SimpleCommand;
import com.axibase.tsd.query.Query;
import com.axibase.tsd.query.QueryPart;

import java.util.*;

import static com.axibase.tsd.client.RequestProcessor.*;
import static com.axibase.tsd.util.AtsdUtil.*;

/**
 * Provides high-level API to retrieve and update ATSD Metadata Objects (entities, entity groups, metrics).
 *
 * @author Nikolay Malevanny.
 */
public class MetaDataService {
    private HttpClientManager httpClientManager;

    public MetaDataService() {
    }

    public MetaDataService(HttpClientManager httpClientManager) {
        this.httpClientManager = httpClientManager;
    }

    public void setHttpClientManager(HttpClientManager httpClientManager) {
        this.httpClientManager = httpClientManager;
    }

    /**
     * @param active      Filter metrics by {@code lastInsertTime}. If active = {@code true}, only metrics with
     *                    positive {@code lastInsertTime} are included in the response.
     * @param expression  Specify EL expression.
     * @param tagAppender Specify metric tags to be included in the response.
     * @param limit       Limit response to first N metrics, ordered by name.
     * @return List of metrics.
     * @throws AtsdClientException
     * @throws AtsdServerException
     */
    public List<Metric> retrieveMetrics(Boolean active,
                                        String expression,
                                        TagAppender tagAppender,
                                        Integer limit) throws AtsdClientException, AtsdServerException {
        QueryPart<Metric> query = new Query<Metric>("metrics")
                .param("active", active)
                .param("expression", expression)
                .param("limit", limit);
        if (tagAppender != null) {
            query = query.param("tags", tagAppender.getTags());
        }
        return httpClientManager.requestMetaDataList(Metric.class, query);
    }

    /**
     * @param entityName  Entity name.
     * @param active      Filter metrics by {@code lastInsertTime}. If active = {@code true}, only metrics with
     *                    positive {@code lastInsertTime} are included in the response.
     * @param expression  Specify EL expression.
     * @param tagAppender Specify metric tags to be included in the response.
     * @param limit       Limit response to first N metrics, ordered by name.
     * @return List of metrics.
     * @throws AtsdClientException
     * @throws AtsdServerException
     */
    public List<Metric> retrieveMetrics(String entityName,
                                        Boolean active,
                                        String expression,
                                        TagAppender tagAppender,
                                        Integer limit) throws AtsdClientException, AtsdServerException {
        QueryPart<Metric> query = new Query<Metric>("entities")
                .path(entityName)
                .path("metrics")
                .param("active", active)
                .param("expression", expression)
                .param("limit", limit);
        if (tagAppender != null) {
            query = query.param("tags", tagAppender.getTags());
        }
        return httpClientManager.requestMetaDataList(Metric.class, query);
    }

    /**
     * @param metricName Metric name.
     * @return Metric.
     * @throws AtsdClientException
     * @throws AtsdServerException
     */
    public Metric retrieveMetric(String metricName)
            throws AtsdClientException, AtsdServerException {
        checkMetricIsEmpty(metricName);
        return httpClientManager.requestMetaDataObject(Metric.class, new Query<Metric>("metrics")
                .path(metricName));
    }

    public boolean createOrReplaceMetric(Metric metric) {
        String metricName = metric.getName();
        checkMetricIsEmpty(metricName);
        QueryPart<Metric> queryPart = new Query<Metric>("metrics")
                .path(metricName);
        return httpClientManager.updateMetaData(queryPart, put(metric));
    }

    public boolean updateMetric(Metric metric) {
        String metricName = metric.getName();
        checkMetricIsEmpty(metricName);
        QueryPart<Metric> queryPart = new Query<Metric>("metrics")
                .path(metricName);
        return httpClientManager.updateMetaData(queryPart, patch(metric));
    }

    public boolean deleteMetric(Metric metric) {
        String metricName = metric.getName();
        checkMetricIsEmpty(metricName);
        QueryPart<Metric> queryPart = new Query<Metric>("metrics")
                .path(metricName);
        return httpClientManager.updateMetaData(queryPart, delete());
    }

    /**
     * @param active      Filter entities by {@code lastInsertTime}. If active = {@code true}, only entities with
     *                    positive {@code lastInsertTime} are included in the response.
     * @param expression  Specify EL expression.
     * @param tagAppender Specify entity tags to be included in the response.
     * @param limit       Limit response to first N entities, ordered by name.
     * @return List of entities.
     * @throws AtsdClientException
     * @throws AtsdServerException
     */
    public List<Entity> retrieveEntities(Boolean active,
                                         String expression,
                                         TagAppender tagAppender,
                                         Integer limit) throws AtsdClientException, AtsdServerException {
        QueryPart<Entity> query = new Query<Entity>("entities")
                .param("active", active)
                .param("expression", expression)
                .param("limit", limit);
        if (tagAppender != null) {
            query = query.param("tags", tagAppender.getTags());
        }
        return httpClientManager.requestMetaDataList(Entity.class, query);
    }

    /**
     * @param entityName Entity name.
     * @return Entity
     * @throws AtsdClientException
     * @throws AtsdServerException
     */
    public Entity retrieveEntity(String entityName)
            throws AtsdClientException, AtsdServerException {
        checkEntityIsEmpty(entityName);
        QueryPart<Entity> query = new Query<Entity>("entities")
                .path(entityName);
        return httpClientManager.requestMetaDataObject(Entity.class, query);
    }

    /**
     * @param entityName entity name
     * @param startTime  to return only property types that have been collected after the specified time
     * @return a set of property types for the entity.
     */
    public Set<String> retrievePropertyTypes(String entityName, Long startTime) {
        checkEntityIsEmpty(entityName);
        QueryPart<String> query = new Query<String>("entities");
        query = query.path(entityName).path("property-types").param("startTime", startTime);
        HashSet<String> result = new HashSet<String>();
        result.addAll(httpClientManager.requestDataList(String.class, query, null));
        return result;
    }

    public boolean createOrReplaceEntity(Entity entity) {
        String entityName = entity.getName();
        checkEntityIsEmpty(entityName);
        QueryPart<Entity> queryPart = new Query<Entity>("entities")
                .path(entityName);
        return httpClientManager.updateMetaData(queryPart, put(entity));
    }

    public boolean updateEntity(Entity entity) {
        String entityName = entity.getName();
        checkEntityIsEmpty(entityName);
        QueryPart<Entity> queryPart = new Query<Entity>("entities")
                .path(entityName);
        return httpClientManager.updateMetaData(queryPart, patch(entity));
    }

    public boolean deleteEntity(Entity entity) {
        String entityName = entity.getName();
        checkEntityIsEmpty(entityName);
        QueryPart<Entity> queryPart = new Query<Entity>("entities")
                .path(entityName);
        return httpClientManager.updateMetaData(queryPart, delete());
    }

    /**
     * @param metricName Metric name.
     * @param entityName Filter entities by entity name.
     * @return List of entities and tags for metric.
     * @throws AtsdClientException
     * @throws AtsdServerException
     */
    public List<EntityAndTags> retrieveEntityAndTags(String metricName, String entityName)
            throws AtsdClientException, AtsdServerException {
        checkMetricIsEmpty(metricName);
        return httpClientManager.requestMetaDataList(EntityAndTags.class, new Query<EntityAndTags>("metrics")
                .path(metricName)
                .path("entity-and-tags")
                .param("entity", entityName)
        );
    }

    /**
     * @return List of entity groups.
     * @throws AtsdClientException
     * @throws AtsdServerException
     */
    public List<EntityGroup> retrieveEntityGroups() throws AtsdClientException, AtsdServerException {
        return httpClientManager.requestMetaDataList(EntityGroup.class, new Query<EntityGroup>("entity-groups"));
    }

    /**
     * @param entityGroupName Entity group name.
     * @return List of entity groups.
     * @throws AtsdClientException
     * @throws AtsdServerException
     */
    public EntityGroup retrieveEntityGroup(String entityGroupName) throws AtsdClientException, AtsdServerException {
        checkEntityGroupIsEmpty(entityGroupName);
        QueryPart<EntityGroup> query = new Query<EntityGroup>("entity-groups")
                .path(entityGroupName);
        return httpClientManager.requestMetaDataObject(EntityGroup.class, query);
    }

    /**
     * Create an entity group with specified properties and tags or replace an existing entity group.
     * <p/>
     * This method creates a new entity group or replaces an existing entity group.
     * <p/>
     * If only a subset of fields is provided for an existing entity group, the remaining properties
     * and tags will be deleted.
     *
     * @param entityGroup the entity group to create or replace
     * @return {@code true} if entity group is created or updated.
     * @throws AtsdClientException
     * @throws AtsdServerException
     */
    public boolean createOrReplaceEntityGroup(EntityGroup entityGroup) throws AtsdClientException, AtsdServerException {
        String entityGroupName = entityGroup.getName();
        checkEntityGroupIsEmpty(entityGroupName);
        QueryPart<EntityGroup> query = new Query<EntityGroup>("entity-groups")
                .path(entityGroupName);
        return httpClientManager.updateMetaData(query, put(entityGroup));
    }

    /**
     * Update specified properties and tags for the given entity group.
     * <p/>
     * This method updates specified properties and tags for an existing entity group.
     * <p/>
     * Properties and tags that are not specified are left unchanged.
     *
     * @param entityGroup the entity group to update
     * @return {@code true} if entity group is updated.
     * @throws AtsdClientException
     * @throws AtsdServerException
     */
    public boolean updateEntityGroup(EntityGroup entityGroup) throws AtsdClientException, AtsdServerException {
        String entityGroupName = entityGroup.getName();
        checkEntityGroupIsEmpty(entityGroupName);
        QueryPart<EntityGroup> query = new Query<EntityGroup>("entity-groups")
                .path(entityGroupName);
        return httpClientManager.updateMetaData(query, patch(entityGroup));
    }

    public boolean deleteEntityGroup(EntityGroup entityGroup) throws AtsdClientException, AtsdServerException {
        String entityGroupName = entityGroup.getName();
        checkEntityGroupIsEmpty(entityGroupName);
        QueryPart<EntityGroup> query = new Query<EntityGroup>("entity-groups")
                .path(entityGroupName);
        return httpClientManager.updateMetaData(query, delete());
    }

    /**
     * @param entityGroupName Entity group name.
     * @param active          Filter entities by {@code lastInsertTime}. If active = {@code true}, only entities with
     *                        positive {@code lastInsertTime} are included in the response.
     * @param expression      Specify EL expression.
     * @param tagAppender     Specify entity tags to be included in the response.
     * @return List of entities for an entity group.
     * @throws AtsdClientException
     * @throws AtsdServerException
     */
    public List<Entity> retrieveGroupEntities(String entityGroupName,
                                              Boolean active,
                                              String expression,
                                              TagAppender tagAppender,
                                              Integer limit) throws AtsdClientException, AtsdServerException {
        checkEntityGroupIsEmpty(entityGroupName);
        QueryPart<Entity> query = new Query<Entity>("entity-groups")
                .path(entityGroupName)
                .path("entities")
                .param("active", active)
                .param("expression", expression)
                .param("limit", limit);
        if (tagAppender != null) {
            query = query.param("tags", tagAppender.getTags());
        }
        return httpClientManager.requestMetaDataList(Entity.class, query);
    }

    /**
     * @param entityGroupName Entity group name.
     * @return List of entities for an entity group.
     * @throws AtsdClientException
     * @throws AtsdServerException
     */
    public List<Entity> retrieveGroupEntities(String entityGroupName)
            throws AtsdClientException, AtsdServerException {
        checkEntityGroupIsEmpty(entityGroupName);
        QueryPart<Entity> query = new Query<Entity>("entity-groups")
                .path(entityGroupName)
                .path("entities");
        query = query.param("tags", TagAppender.ALL.getTags());
        return httpClientManager.requestMetaDataList(Entity.class, query);
    }

    /**
     * Add specified entities to entity group.
     *
     * @param entityGroupName Entity group name.
     * @param createEntities  Automatically create new entities from the submitted list if such entities don't already exist.
     * @param entities        Entities to create.
     * @return {@code true} if entities added.
     * @throws AtsdClientException if there is any client problem
     * @throws AtsdServerException if there is any server problem
     */
    public boolean addGroupEntities(String entityGroupName, Boolean createEntities, Entity... entities) {
        checkEntityGroupIsEmpty(entityGroupName);
        List<String> entitiesNames = new ArrayList<>();
        for (Entity entity : entities) {
            entitiesNames.add(entity.getName());
        }
        QueryPart<EntityGroup> query = new Query<EntityGroup>("entity-groups")
                .path(entityGroupName)
                .path("entities/add")
                .param("createEntities", createEntities);
        return httpClientManager.updateData(query, post(entitiesNames));
    }

    /**
     * Replace entities in the entity group with the specified collection.
     * All existing entities that are not included in the collection will be removed.
     * If the specified collection is empty, all entities are removed from the group (replace with empty collection).
     *
     * @param entityGroupName Entity group name.
     * @param createEntities  Automatically create new entities from the submitted list if such entities don't already exist.
     * @param entities        Entities to replace.
     * @return {@code true} if entities replaced.
     * @throws AtsdClientException if there is any client problem
     * @throws AtsdServerException if there is any server problem
     */
    public boolean replaceGroupEntities(String entityGroupName, Boolean createEntities, Entity... entities) {
        checkEntityGroupIsEmpty(entityGroupName);
        List<String> entitiesNames = new ArrayList<>();
        for (Entity entity : entities) {
            entitiesNames.add(entity.getName());
        }
        QueryPart<Entity> query = new Query<Entity>("entity-groups")
                .path(entityGroupName)
                .path("entities/set")
                .param("createEntities", createEntities);
        return httpClientManager.updateMetaData(query, post(entitiesNames));
    }

    /**
     * Delete entities from entity group.
     *
     * @param entityGroupName Entity group name.
     * @param entities        Entities to replace.  @return {@code true} if entities added.
     * @throws AtsdClientException if there is any client problem
     * @throws AtsdServerException if there is any server problem
     */
    public boolean deleteGroupEntities(String entityGroupName, Entity... entities) {
        checkEntityGroupIsEmpty(entityGroupName);
        List<String> entitiesNames = new ArrayList<>();
        for (Entity entity : entities) {
            entitiesNames.add(entity.getName());
        }
        QueryPart<Entity> query = new Query<Entity>("entity-groups")
                .path(entityGroupName)
                .path("entities/delete");
        return httpClientManager.updateMetaData(query, post(entitiesNames));
    }

    /**
     * Delete all entities from entity group.
     *
     * @param entityGroupName Entity group name.
     * @throws AtsdClientException if there is any client problem
     * @throws AtsdServerException if there is any server problem
     */
    public boolean deleteAllGroupEntities(String entityGroupName) {
        return replaceGroupEntities(entityGroupName, true);
    }
}
