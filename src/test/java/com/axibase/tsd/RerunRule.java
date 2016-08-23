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

package com.axibase.tsd;

import com.axibase.tsd.client.AtsdClientException;
import junit.framework.AssertionFailedError;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.axibase.tsd.TestUtil.RERUN_COUNT;

/**
 * @author Nikolay Malevanny.
 */
public class RerunRule implements TestRule {
    private static final Logger log = LoggerFactory.getLogger(RerunRule.class);

    public Statement apply(Statement base, Description description) {
        return statement(base);
    }

    private Statement statement(final Statement base) {

        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                for (int i = 0; i < RERUN_COUNT; i++) {
                    try {
                        base.evaluate();
                        return;
                    } catch (AtsdClientException ex) {
                        onError(i, ex);
                    } catch (AssertionFailedError ex) {
                        onError(i, ex);
                    }
                }
            }
        };
    }

    private void onError(int i, Throwable ex) throws Throwable {
        log.warn("Ignore possible connectivity error: ", ex);
        if (i == RERUN_COUNT - 1) {
            throw ex;
        }
    }
}
