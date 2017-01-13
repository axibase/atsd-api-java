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

import com.axibase.tsd.model.data.series.Series;
import com.axibase.tsd.model.meta.*;
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

    /**
     * constructor
     */
    public MetaDataService() {
        //
    }

    /**
     * constructor
     *
     * @param httpClientManager client
     */
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
     * @throws AtsdClientException raised  raised
     * @throws AtsdServerException raised  raised
     * @deprecated use {@link #retrieveMetrics(String, String, String, TagAppender, Integer)} instead.
     */
    @Deprecated
    public List<Metric> retrieveMetrics(Boolean active,
                                        String expression,
                                        TagAppender tagAppender,
                                        Integer limit) {
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
     * @param expression    Specify EL expression.
     * @param minInsertDate Include metrics with lastInsertDate equal or greater than specified time.
     *                      Time can be specified in ISO format or using <a href="https://github.com/axibase/atsd-docs/blob/master/end-time-syntax.md">endtime</a> syntax.
     * @param maxInsertDate Include metrics with lastInsertDate less than specified time.
     *                      Time can be specified in ISO format or using <a href="https://github.com/axibase/atsd-docs/blob/master/end-time-syntax.md">endtime</a> syntax.
     * @param tagAppender   Specify metric tags to be included in the response. Specify * to include all metric tags.
     * @param limit         Limit response to first N metrics, ordered by name.
     * @return List of metrics.
     * @throws AtsdClientException raised  raised
     * @throws AtsdServerException raised  raised
     */
    public List<Metric> retrieveMetrics(String expression,
                                        String minInsertDate,
                                        String maxInsertDate,
                                        TagAppender tagAppender,
                                        Integer limit) {
        QueryPart<Metric> query = new Query<Metric>("metrics")
                .param("expression", expression)
                .param("minInsertDate", minInsertDate)
                .param("maxInsertDate", maxInsertDate)
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
     * @deprecated use {@link #retrieveMetrics(String, String, String, String, Boolean, TagAppender, Integer)} instead.
     */
    @Deprecated
    public List<Metric> retrieveMetrics(String entityName,
                                        Boolean active,
                                        String expression,
                                        TagAppender tagAppender,
                                        Integer limit) {
        QueryPart<Metric> query = new Query<Metric>("entities")
                .path(entityName, true)
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
     * @param entityName          Entity name.
     * @param expression          Specify EL expression.
     * @param minInsertDate       Include metrics with lastInsertDate equal or greater than specified time.
     *                            Time can be specified in ISO format or using <a href="https://github.com/axibase/atsd-docs/blob/master/end-time-syntax.md">endtime</a> syntax.
     * @param maxInsertDate       Include metrics with lastInsertDate less than specified time.
     *                            Time can be specified in ISO format or using <a href="https://github.com/axibase/atsd-docs/blob/master/end-time-syntax.md">endtime</a> syntax.
     * @param useEntityInsertTime If true, lastInsertDate is calculated for the specified entity and metric.
     *                            Otherwise, lastInsertDate represents the last time for all entities. Default: false.
     * @param tagAppender         Specify metric tags to be included in the response. Specify * to include all metric tags.
     * @param limit               Limit response to first N metrics, ordered by name.
     * @return List of metrics.
     */
    public List<Metric> retrieveMetrics(String entityName,
                                        String expression,
                                        String minInsertDate,
                                        String maxInsertDate,
                                        Boolean useEntityInsertTime,
                                        TagAppender tagAppender,
                                        Integer limit) {
        QueryPart<Metric> query = new Query<Metric>("entities")
                .path(entityName, true)
                .path("metrics")
                .param("expression", expression)
                .param("minInsertDate", minInsertDate)
                .param("maxInsertDate", maxInsertDate)
                .param("useEntityInsertTime", useEntityInsertTime)
                .param("limit", limit);
        if (tagAppender != null) {
            query = query.param("tags", tagAppender.getTags());
        }
        return httpClientManager.requestMetaDataList(Metric.class, query);
    }

    /**
     * @param metricName Metric name.
     * @return Metric.
     */
    public Metric retrieveMetric(String metricName) {
        checkMetricIsEmpty(metricName);
        return httpClientManager.requestMetaDataObject(Metric.class, new Query<Metric>("metrics")
                .path(metricName, true));
    }

    /**
     * create or replace metric
     *
     * @param metric metric
     * @return is success
     */
    public boolean createOrReplaceMetric(Metric metric) {
        String metricName = metric.getName();
        checkMetricIsEmpty(metricName);
        QueryPart<Metric> queryPart = new Query<Metric>("metrics")
                .path(metricName, true);
        return httpClientManager.updateMetaData(queryPart, put(metric));
    }


    /**
     * update metric
     *
     * @param metric metric update fields
     * @return is success
     */
    public boolean updateMetric(Metric metric) {
        String metricName = metric.getName();
        checkMetricIsEmpty(metricName);
        QueryPart<Metric> queryPart = new Query<Metric>("metrics")
                .path(metricName, true);
        return httpClientManager.updateMetaData(queryPart, patch(metric));
    }


    /**
     * delete metric
     *
     * @param metric metric
     * @return is success
     */
    public boolean deleteMetric(Metric metric) {
        String metricName = metric.getName();
        checkMetricIsEmpty(metricName);
        QueryPart<Metric> queryPart = new Query<Metric>("metrics")
                .path(metricName, true);
        return httpClientManager.updateMetaData(queryPart, delete());
    }

    /**
     * @param active      Filter entities by {@code lastInsertTime}. If active = {@code true}, only entities with
     *                    positive {@code lastInsertTime} are included in the response.
     * @param expression  Specify EL expression.
     * @param tagAppender Specify entity tags to be included in the response.
     * @param limit       Limit response to first N entities, ordered by name.
     * @return List of entities.
     * @deprecated use {@link #retrieveEntities(String, String, String, TagAppender, Integer)} instead.
     */
    @Deprecated
    public List<Entity> retrieveEntities(Boolean active,
                                         String expression,
                                         TagAppender tagAppender,
                                         Integer limit) {
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
     * @param expression    Specify EL expression.
     * @param minInsertDate Include entities with lastInsertDate equal or greater than specified time.
     *                      Time can be specified in ISO format or using <a href="https://github.com/axibase/atsd-docs/blob/master/end-time-syntax.md">endtime</a> syntax.
     * @param maxInsertDate Include entities with lastInsertDate less than specified time.
     *                      Time can be specified in ISO format or using <a href="https://github.com/axibase/atsd-docs/blob/master/end-time-syntax.md">endtime</a> syntax.
     * @param tagAppender   Specify entity tags to be included in the response. Specify * to include all entity tags.
     * @param limit         Limit response to first N entities, ordered by name.
     * @return List of entities.
     */
    public List<Entity> retrieveEntities(String expression,
                                         String minInsertDate,
                                         String maxInsertDate,
                                         TagAppender tagAppender,
                                         Integer limit) {
        QueryPart<Entity> query = new Query<Entity>("entities")
                .param("expression", expression)
                .param("minInsertDate", minInsertDate)
                .param("maxInsertDate", maxInsertDate)
                .param("limit", limit);
        if (tagAppender != null) {
            query = query.param("tags", tagAppender.getTags());
        }
        return httpClientManager.requestMetaDataList(Entity.class, query);
    }

    /**
     * @param entityName Entity name.
     * @return Entity
     */
    public Entity retrieveEntity(String entityName) {
        checkEntityIsEmpty(entityName);
        QueryPart<Entity> query = new Query<Entity>("entities")
                .path(entityName, true);
        return httpClientManager.requestMetaDataObject(Entity.class, query);
    }

    /**
     * @param entityName entity name
     * @param startTime  to return only property types that have been collected after the specified time
     * @return a set of property types for the entity.
     */
    public Set<String> retrievePropertyTypes(String entityName, Long startTime) {
        checkEntityIsEmpty(entityName);
        QueryPart<String> query = new Query<>("entities");
        query = query.path(entityName, true).path("property-types").param("startTime", startTime);
        HashSet<String> result = new HashSet<>();
        result.addAll(httpClientManager.requestDataList(String.class, query, null));
        return result;
    }

    /**
     * Create or replace
     *
     * @param entity entity
     * @return is success
     */
    public boolean createOrReplaceEntity(Entity entity) {
        String entityName = entity.getName();
        checkEntityIsEmpty(entityName);
        QueryPart<Entity> queryPart = new Query<Entity>("entities")
                .path(entityName, true);
        return httpClientManager.updateMetaData(queryPart, put(entity));
    }

    /**
     * update Entity
     *
     * @param entity entity
     * @return is success
     */
    public boolean updateEntity(Entity entity) {
        String entityName = entity.getName();
        checkEntityIsEmpty(entityName);
        QueryPart<Entity> queryPart = new Query<Entity>("entities")
                .path(entityName, true);
        return httpClientManager.updateMetaData(queryPart, patch(entity));
    }

    /**
     * Delete entity
     *
     * @param entity entity
     * @return is success
     */
    public boolean deleteEntity(Entity entity) {
        String entityName = entity.getName();
        checkEntityIsEmpty(entityName);
        QueryPart<Entity> queryPart = new Query<Entity>("entities")
                .path(entityName, true);
        return httpClientManager.updateMetaData(queryPart, delete());
    }

    /**
     * @param metricName Metric name.
     * @param entityName Filter entities by entity name.
     * @return List of entities and tags for metric.
     */
    public List<EntityAndTags> retrieveEntityAndTags(String metricName, String entityName) {
        checkMetricIsEmpty(metricName);
        return httpClientManager.requestMetaDataList(EntityAndTags.class, new Query<EntityAndTags>("metrics")
                .path(metricName, true)
                .path("entity-and-tags")
                .param("entity", entityName)
        );
    }

    /**
     * @return List of entity groups.
     */
    public List<EntityGroup> retrieveEntityGroups() {
        return httpClientManager.requestMetaDataList(EntityGroup.class, new Query<EntityGroup>("entity-groups"));
    }

    /**
     * @param entityGroupName Entity group name.
     * @return List of entity groups.
     */
    public EntityGroup retrieveEntityGroup(String entityGroupName) {
        checkEntityGroupIsEmpty(entityGroupName);
        QueryPart<EntityGroup> query = new Query<EntityGroup>("entity-groups")
                .path(entityGroupName, true);
        return httpClientManager.requestMetaDataObject(EntityGroup.class, query);
    }

    /**
     * Create an entity group with specified properties and tags or replace an existing entity group.
     * <p>
     * This method creates a new entity group or replaces an existing entity group.
     * </p>
     * If only a subset of fields is provided for an existing entity group, the remaining properties
     * and tags will be deleted.
     *
     * @param entityGroup the entity group to create or replace
     * @return {@code true} if entity group is created or updated.
     */
    public boolean createOrReplaceEntityGroup(EntityGroup entityGroup) {
        String entityGroupName = entityGroup.getName();
        checkEntityGroupIsEmpty(entityGroupName);
        QueryPart<EntityGroup> query = new Query<EntityGroup>("entity-groups")
                .path(entityGroupName, true);
        return httpClientManager.updateMetaData(query, put(entityGroup));
    }

    /**
     * Update specified properties and tags for the given entity group.
     * <p>
     * This method updates specified properties and tags for an existing entity group.
     * </p>
     * Properties and tags that are not specified are left unchanged.
     *
     * @param entityGroup the entity group to update
     * @return {@code true} if entity group is updated.
     */
    public boolean updateEntityGroup(EntityGroup entityGroup) {
        String entityGroupName = entityGroup.getName();
        checkEntityGroupIsEmpty(entityGroupName);
        QueryPart<EntityGroup> query = new Query<EntityGroup>("entity-groups")
                .path(entityGroupName, true);
        return httpClientManager.updateMetaData(query, patch(entityGroup));
    }

    /**
     * Delete entity group
     *
     * @param entityGroup entity group
     * @return is success
     */
    public boolean deleteEntityGroup(EntityGroup entityGroup) {
        String entityGroupName = entityGroup.getName();
        checkEntityGroupIsEmpty(entityGroupName);
        QueryPart<EntityGroup> query = new Query<EntityGroup>("entity-groups")
                .path(entityGroupName, true);
        return httpClientManager.updateMetaData(query, delete());
    }

    /**
     * @param entityGroupName Entity group name.
     * @param active          Filter entities by {@code lastInsertTime}. If active = {@code true}, only entities with
     *                        positive {@code lastInsertTime} are included in the response.
     * @param expression      Specify EL expression.
     * @param tagAppender     Specify entity tags to be included in the response.
     * @param limit           limit
     * @return List of entities for an entity group.
     * @deprecated use {@link #retrieveGroupEntities(String, String, String, String, TagAppender, Integer)} instead.
     */
    @Deprecated
    public List<Entity> retrieveGroupEntities(String entityGroupName,
                                              Boolean active,
                                              String expression,
                                              TagAppender tagAppender,
                                              Integer limit) {
        checkEntityGroupIsEmpty(entityGroupName);
        QueryPart<Entity> query = new Query<Entity>("entity-groups")
                .path(entityGroupName, true)
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
     * @param expression      Specify EL expression.
     * @param minInsertDate   Include entities with lastInsertDate equal or greater than specified time.
     *                        Time can be specified in ISO format or using <a href="https://github.com/axibase/atsd-docs/blob/master/end-time-syntax.md">endtime</a> syntax.
     * @param maxInsertDate   Include entities with lastInsertDate less than specified time.
     *                        Time can be specified in ISO format or using <a href="https://github.com/axibase/atsd-docs/blob/master/end-time-syntax.md">endtime</a> syntax.
     * @param tagAppender     Specify entity tags to be included in the response. Specify * to include all entity tags.
     * @param limit           limit
     * @return List of entities for an entity group.
     */
    public List<Entity> retrieveGroupEntities(String entityGroupName,
                                              String expression,
                                              String minInsertDate,
                                              String maxInsertDate,
                                              TagAppender tagAppender,
                                              Integer limit) {
        checkEntityGroupIsEmpty(entityGroupName);
        QueryPart<Entity> query = new Query<Entity>("entity-groups")
                .path(entityGroupName, true)
                .path("entities")
                .param("expression", expression)
                .param("minInsertDate", minInsertDate)
                .param("maxInsertDate", maxInsertDate)
                .param("limit", limit);
        if (tagAppender != null) {
            query = query.param("tags", tagAppender.getTags());
        }
        return httpClientManager.requestMetaDataList(Entity.class, query);
    }

    /**
     * @param entityGroupName Entity group name.
     * @return List of entities for an entity group.
     * @throws AtsdClientException raised
     * @throws AtsdServerException raised
     */
    public List<Entity> retrieveGroupEntities(String entityGroupName) {
        checkEntityGroupIsEmpty(entityGroupName);
        QueryPart<Entity> query = new Query<Entity>("entity-groups")
                .path(entityGroupName, true)
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
     * @throws AtsdClientException raised if there is any client problem
     * @throws AtsdServerException raised if there is any server problem
     */
    public boolean addGroupEntities(String entityGroupName, Boolean createEntities, Entity... entities) {
        checkEntityGroupIsEmpty(entityGroupName);
        List<String> entitiesNames = new ArrayList<>();
        for (Entity entity : entities) {
            entitiesNames.add(entity.getName());
        }
        QueryPart<EntityGroup> query = new Query<EntityGroup>("entity-groups")
                .path(entityGroupName, true)
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
     * @throws AtsdClientException raised if there is any client problem
     * @throws AtsdServerException raised if there is any server problem
     */
    public boolean replaceGroupEntities(String entityGroupName, Boolean createEntities, Entity... entities) {
        checkEntityGroupIsEmpty(entityGroupName);
        List<String> entitiesNames = new ArrayList<>();
        for (Entity entity : entities) {
            entitiesNames.add(entity.getName());
        }
        QueryPart<Entity> query = new Query<Entity>("entity-groups")
                .path(entityGroupName, true)
                .path("entities/set")
                .param("createEntities", createEntities);
        return httpClientManager.updateMetaData(query, post(entitiesNames));
    }

    /**
     * Delete entities from entity group.
     *
     * @param entityGroupName Entity group name.
     * @param entities        Entities to replace.  @return {@code true} if entities added.
     * @return is success
     * @throws AtsdClientException raised if there is any client problem
     * @throws AtsdServerException raised if there is any server problem
     */
    public boolean deleteGroupEntities(String entityGroupName, Entity... entities) {
        checkEntityGroupIsEmpty(entityGroupName);
        List<String> entitiesNames = new ArrayList<>();
        for (Entity entity : entities) {
            entitiesNames.add(entity.getName());
        }
        QueryPart<Entity> query = new Query<Entity>("entity-groups")
                .path(entityGroupName, true)
                .path("entities/delete");
        return httpClientManager.updateMetaData(query, post(entitiesNames));
    }

    /**
     * Delete all entities from entity group.
     *
     * @param entityGroupName Entity group name.
     * @return is success
     * @throws AtsdClientException raised if there is any client problem
     * @throws AtsdServerException raised if there is any server problem
     */
    public boolean deleteAllGroupEntities(String entityGroupName) {
        return replaceGroupEntities(entityGroupName, true);
    }

    /**
     * Retrieve series list of the specified metric
     *
     * @param metricName metric name
     * @return list of series
     */
    public List<Series> retrieveMetricSeries(String metricName) {
        return retrieveMetricSeries(metricName, Collections.<String, String>emptyMap());
    }

    /**
     * Retrieve series list of the specified metric
     *
     * @param metricName metric name
     * @param entityName entity name's filter
     * @return list of series
     */
    public List<Series> retrieveMetricSeries(String metricName, String entityName) {
        return retrieveMetricSeries(metricName, Collections.singletonMap("entity", entityName));
    }

    private List<Series> retrieveMetricSeries(String metricName, Map<String, String> queryParams) {
        QueryPart<Series> query = new Query<Series>("metrics")
                .path(metricName)
                .path("series");
        for (Map.Entry<String, String> param : queryParams.entrySet()) {
            query.param(param.getKey(), param.getValue());
        }
        return httpClientManager.requestMetaDataList(Series.class, query);
    }
}