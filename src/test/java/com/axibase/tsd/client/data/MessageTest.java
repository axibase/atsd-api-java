/*
 *
 *  * Copyright 2016 Axibase Corporation or its affiliates. All Rights Reserved.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License").
 *  * You may not use this file except in compliance with the License.
 *  * A copy of the License is located at
 *  *
 *  * https://www.axibase.com/atsd/axibase-apache-2.0.pdf
 *  *
 *  * or in the "license" file accompanying this file. This file is distributed
 *  * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  * express or implied. See the License for the specific language governing
 *  * permissions and limitations under the License.
 *
 */
package com.axibase.tsd.client.data;

import com.axibase.tsd.RerunRule;
import com.axibase.tsd.client.DataService;
import com.axibase.tsd.client.HttpClientManager;
import com.axibase.tsd.model.data.Message;
import com.axibase.tsd.model.data.Severity;
import com.axibase.tsd.model.data.command.BatchResponse;
import com.axibase.tsd.model.data.command.GetMessagesQuery;
import com.axibase.tsd.network.MessageInsertCommand;
import com.axibase.tsd.network.PlainCommand;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.*;

import static com.axibase.tsd.TestUtil.*;
import static junit.framework.Assert.*;

public class MessageTest {

    private DataService dataService;
    private HttpClientManager httpClientManager;

    @Rule
    public RerunRule rerunRule = new RerunRule();

    @Before
    public void setUp() throws Exception {
        httpClientManager = buildHttpClientManager();
        httpClientManager.setCheckPeriodMillis(1000);
//        httpClientManager.setCheckPeriodMillis(30); // to extreme tests
        dataService = new DataService();
        dataService.setHttpClientManager(httpClientManager);

        waitWorkingServer(httpClientManager);
    }


    @Test
    public void testInsertMessages() throws Exception {
        final String entityName = buildVariablePrefix() + "entity";
        final String messageTextUnknown = "message txt 1";
        final String messageTextMinor = "message text 2";
        final String messageTextCritical = "message text 3";
        final long timestamp = System.currentTimeMillis();

        final Message m1 = new Message(entityName, messageTextUnknown)
                .setSeverity(Severity.UNKNOWN)
                .setTimestamp(timestamp);

        final Message m2 = new Message(entityName, messageTextMinor)
                .setSeverity(Severity.MINOR)
                .setTimestamp(timestamp);

        final Message m3 = new Message(entityName, messageTextCritical)
                .setSeverity(Severity.CRITICAL)
                .setTimestamp(timestamp);
        assertTrue(dataService.insertMessages(m1, m2, m3));

        Thread.sleep(WAIT_TIME);

        GetMessagesQuery getMessagesQuery = new GetMessagesQuery(Arrays.asList(entityName))
                .setStartDate(new Date(timestamp - 1)).setEndDate(new Date(timestamp + 1));

        List<Message> messages = dataService.retrieveMessages(getMessagesQuery);
        assertNotNull(messages);
        assertEquals(3, messages.size());

        boolean unknownChecked = false;
        boolean minorChecked = false;
        boolean criticalChecked = false;
        for (Message msg : messages) {
            if (msg.getSeverity() == Severity.UNKNOWN) {
                assertFalse(unknownChecked);
                assertEquals(messageTextUnknown, msg.getMessage());
                unknownChecked = true;

            } else if (msg.getSeverity() == Severity.MINOR) {
                assertFalse(minorChecked);
                assertEquals(messageTextMinor, msg.getMessage());
                minorChecked = true;
            } else if (msg.getSeverity() == Severity.CRITICAL) {
                assertFalse(criticalChecked);
                assertEquals(messageTextCritical, msg.getMessage());
                criticalChecked = true;
            }
        }
        assertTrue(unknownChecked);
        assertTrue(minorChecked);
        assertTrue(criticalChecked);
    }

    @Test
    public void testRetrieveMessagesByEntityName() throws Exception {
        final String entityName = buildVariablePrefix() + "entity";
        final String messageTextUnknown = "message txt 1";
        final long timestamp = MOCK_TIMESTAMP;
        final long delta = 1L;

        GetMessagesQuery getMessagesQuery = new GetMessagesQuery(entityName).setStartDate(new Date(timestamp - delta)).setEndDate(new Date(timestamp + delta));

        if (dataService.retrieveMessages(getMessagesQuery).isEmpty()) {
            Message message = new Message(entityName, messageTextUnknown).setSeverity(Severity.UNKNOWN).setTimestamp(timestamp);
            assertTrue(dataService.insertMessages(message));
        }

        List messages = dataService.retrieveMessages(getMessagesQuery);
        assertFalse(messages.isEmpty());
        assertTrue(messages.get(0) instanceof Message);
        assertEquals(entityName, ((Message) messages.get(0)).getEntityName());
    }

    @Test
    public void testRetrieveMessagesByEntitiesName() throws Exception {
        final String messageTextUnknown = "message txt 1";
        final long timestamp = MOCK_TIMESTAMP;
        buildVariablePrefix();
        final List<String> entitiesName = Arrays.asList(buildVariablePrefix() + "entity-first", buildVariablePrefix() + "entity-second");

        GetMessagesQuery getMessagesQuery = new GetMessagesQuery(entitiesName).setStartDate(new Date(timestamp));
        if (dataService.retrieveMessages(getMessagesQuery).size() < 2) {
            for (String entityName : entitiesName) {
                Message message = new Message(entityName, messageTextUnknown).setSeverity(Severity.UNKNOWN).setTimestamp(timestamp);
                assertTrue(dataService.insertMessages(message));
            }
            Thread.sleep(WAIT_TIME);
        }

        List<Message> messages = dataService.retrieveMessages(getMessagesQuery);
        assertFalse(messages.isEmpty());
        assertTrue(messages.get(0) instanceof Message);

        assertFalse(messages.isEmpty());

        final List<String> recivedEntitiesName = new ArrayList<>();
        for (Message msg : messages) {
            recivedEntitiesName.add(msg.getEntityName());
        }
        assertTrue(recivedEntitiesName.containsAll(entitiesName));
    }

    @Test
    public void testSendBatch() throws Exception {
        final String entityName = buildVariablePrefix() + "entity";
        final String messageText = "message txt 1";
        final long st = System.currentTimeMillis();
        final ArrayList<PlainCommand> commands = new ArrayList<>();
        commands.add(new MessageInsertCommand(entityName, st, Collections.<String, String>emptyMap(), messageText));
        commands.add(new MessageInsertCommand(entityName, st+2, Collections.singletonMap("tag1", "value1"), messageText));
        commands.add(new MessageInsertCommand(entityName, st+3, Collections.singletonMap("tag1", "value1"), ""));

        final BatchResponse batchResponse = dataService.sendBatch(commands);
        assertTrue(batchResponse.getResult().getFail() == 0);

        Thread.sleep(WAIT_TIME);

        final GetMessagesQuery getMessagesQuery = new GetMessagesQuery(entityName);
        getMessagesQuery.setStartDate(new Date(st));
        getMessagesQuery.setEndDate(new Date(st + 4));
        final List<Message> messageResults = dataService.retrieveMessages(getMessagesQuery);
        assertEquals(3, messageResults.size());
    }


    @After
    public void tearDown() throws Exception {
        httpClientManager.close();
    }

}