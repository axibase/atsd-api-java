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
package com.axibase.tsd.model.data.command;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.ArrayList;
import java.util.List;


@JsonInclude(JsonInclude.Include.NON_NULL)
public class BatchAlertCommand {
    private final String action;
    private Fields fields;
    private final List<AlertId> alerts = new ArrayList<AlertId>();

    private BatchAlertCommand(String action, String[] alertIds) {
        this.action = action;
        for (int i = 0; i < alertIds.length; i++) {
            String alertId = alertIds[i];
            alerts.add(new AlertId(alertId));
        }
    }

    public String getAction() {
        return action;
    }

    public Fields getFields() {
        return fields;
    }

    public List<AlertId> getAlerts() {
        return alerts;
    }

    public static BatchAlertCommand createUpdateCommand(boolean acknowledge, String... alertIds) {
        BatchAlertCommand insertCommand = new BatchAlertCommand("update", alertIds);
        insertCommand.fields = new Fields(acknowledge);
        return insertCommand;
    }

    public static BatchAlertCommand createDeleteCommand(String... alertIds) {
        BatchAlertCommand deleteCommand = new BatchAlertCommand("delete", alertIds);
        return deleteCommand;
    }

    public static class Fields {
        private final boolean acknowledge;

        public Fields(boolean acknowledge) {
            this.acknowledge = acknowledge;
        }

        public boolean isAcknowledge() {
            return acknowledge;
        }
    }

    public static class AlertId {
        private final String id;

        public AlertId(String id) {
            this.id = id;
        }

        public String getId() {
            return id;
        }
    }
}
