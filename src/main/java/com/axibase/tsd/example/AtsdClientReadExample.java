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

package com.axibase.tsd.example;

import com.axibase.tsd.model.data.command.GetSeriesQuery;
import com.axibase.tsd.model.data.command.SimpleAggregateMatcher;
import com.axibase.tsd.model.data.series.Series;
import com.axibase.tsd.model.data.series.Interpolate;
import com.axibase.tsd.model.data.series.Interval;
import com.axibase.tsd.model.data.series.IntervalUnit;
import com.axibase.tsd.model.data.series.aggregate.AggregateType;
import com.axibase.tsd.model.meta.EntityAndTags;
import com.axibase.tsd.model.meta.Metric;

import java.util.List;
import java.util.Map;




public class AtsdClientReadExample extends AbstractAtsdClientExample {

    public static void main(String[] args) {
        AtsdClientReadExample atsdClientReadExample = new AtsdClientReadExample();
        atsdClientReadExample.configure();
        atsdClientReadExample.printMetaDataAndData();
    }

    private void printMetaDataAndData() {
        String metricExample = "jvm_memory_used_percent";
//        String metricExample = "disk_used_percent";
        Metric metric = metaDataService.retrieveMetric(metricExample);
        if (metric == null) {
            logger.info("Unknown metric: " + metricExample);
            return;
        }
        List<EntityAndTags> entityAndTagsList = metaDataService.retrieveEntityAndTags(metric.getName(), null);
        logger.info("===Metric MetaData===");
        logger.info("Metric: " + metric);
        for (EntityAndTags entityAndTags : entityAndTagsList) {
            String entityName = entityAndTags.getEntityName();
            logger.info("\n===Entity MetaData===");
            logger.info("Entity: " + entityName);
            Map<String, String> tags = entityAndTags.getTags();
            logger.info("===Tags===");
            for (Map.Entry<String, String> tagAndValue : tags.entrySet()) {
                logger.info("\t" + tagAndValue.getKey() + " : " + tagAndValue.getValue());
            }

            logger.info("===Sample===");
            GetSeriesQuery command = new GetSeriesQuery(entityName, metric.getName(), tags,
                    System.currentTimeMillis() - 3600, System.currentTimeMillis());
            command.setAggregateMatcher(new SimpleAggregateMatcher(new Interval(1, IntervalUnit.MINUTE),
                    Interpolate.NONE,
                    AggregateType.DETAIL));
            List<Series> getSeriesResults = dataService.retrieveSeries(command);
            for (Series getSeriesResult : getSeriesResults) {
                print(getSeriesResult);
            }
        }
    }


}

