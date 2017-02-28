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

import com.axibase.tsd.client.AtsdClientException;
import org.apache.commons.lang3.StringUtils;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;


public abstract class AbstractQueryPart<T> implements QueryPart<T> {
    private static String encode(String str) {
        try {
            return URLEncoder.encode(str, StandardCharsets.UTF_8.name()).replace("+", "%20");
        } catch (UnsupportedEncodingException e) {
            throw new AtsdClientException("Encode error", e);
        }
    }

    @Override
    public QueryPart<T> param(String name, Object value) {
        if (value != null) {
            return new QueryParam<>(name, value, this);
        } else {
            return this;
        }
    }

    @Override
    public QueryPart<T> path(String path) {
        return path(path, false);
    }

    @Override
    public QueryPart<T> path(String path, boolean encode) {
        if (StringUtils.isBlank(path)) {
            throw new IllegalArgumentException("Path element is empty: " + path);
        }
        return new Query<>(encode ? encode(path) : path, this);
    }

}