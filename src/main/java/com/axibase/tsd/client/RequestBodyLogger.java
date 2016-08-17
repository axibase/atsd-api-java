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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.ClientRequestContext;
import javax.ws.rs.client.ClientRequestFilter;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;


class RequestBodyLogger implements ClientRequestFilter {
    private static final Logger log = LoggerFactory.getLogger(RequestBodyLogger.class);

    @Override
    public void filter(ClientRequestContext clientRequestContext) throws IOException {
        OutputStream outputStream = new FilterOutputStream(clientRequestContext.getEntityStream()) {
            @Override
            public void write(byte[] bytes, int i, int i1) throws IOException {
                if (log.isDebugEnabled()) {
                    log.debug(new String(bytes, i, i1));
                }
                super.write(bytes, i, i1);
            }
        };
        clientRequestContext.setEntityStream(outputStream);
    }
}
