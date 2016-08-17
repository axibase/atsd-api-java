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
package com.axibase.tsd.query;

import org.apache.commons.lang3.StringUtils;


public abstract class AbstractQueryPart<T> implements QueryPart<T> {
    public QueryPart<T> param(String name, Object value) {
        if (value != null) {
            return new QueryParam<T>(name, value, this);
        } else {
            return this;
        }
    }

    public QueryPart<T> path(String path) {
        if (StringUtils.isBlank(path)) {
            throw new IllegalArgumentException("Path element is empty: " + path);
        }
        return new Query<T>(path, this);
    }
}
