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

package com.axibase.tsd.model.system;


public class MarkerState {
    private String marker;
    private Integer count;

    public String getMarker() {
        return marker;
    }

    public Integer getCount() {
        return count;
    }

    public void setMarker(String marker) {
        this.marker = marker;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "MarkerState{" +
                "marker='" + marker + '\'' +
                ", count=" + count +
                '}';
    }
}
