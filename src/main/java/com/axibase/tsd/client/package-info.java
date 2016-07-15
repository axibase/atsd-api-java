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
/**
 * Provides classes that implement all the major Axibase Time-Sample Database operations.  The actual classes used
 * as return values or parameters are defined in the com.axibase.tsd.model package.
 *
 * <ul>
 *     <li><b>MetaDataService</b> api is used for querying a metadata objects (metric, entity, entity group).</li>
 *     <li><b>DataService</b> api is used for querying a data objects (alert, property, time series)</li>
 * </ul>
 **/
package com.axibase.tsd.client;