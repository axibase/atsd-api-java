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
package com.axibase.tsd.model.meta;

import org.apache.commons.lang3.StringUtils;


public class TagAppender {
    public static final TagAppender ALL = new TagAppender("*");
    private final String tags;

    private TagAppender(String tags) {
        this.tags = tags;
    }

    public static TagAppender createTagAppender(String... tagNames) {
        if (tagNames == null || tagNames.length == 0) {
            throw new IllegalArgumentException("Tag names list could not be null or empty");
        }
        return new TagAppender(StringUtils.join(tagNames, ','));
    }

    public String getTags() {
        return tags;
    }
}
