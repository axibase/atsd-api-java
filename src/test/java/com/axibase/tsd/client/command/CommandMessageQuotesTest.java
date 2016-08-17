package com.axibase.tsd.client.command;

import com.axibase.tsd.model.data.Message;
import com.axibase.tsd.network.MessageInsertCommand;
import com.axibase.tsd.network.PlainCommand;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertEquals;


public class CommandMessageQuotesTest {
    private final static String TEST_PREFIX = "command-message-quotes-test-";
    private final static String TEST_ENTITY = TEST_PREFIX + "entity";
    private static Message testMessage;

    @BeforeClass
    public static void prepareData() {
        Map<String, String> tags = Collections.singletonMap("tag", "OFF- RAMP \" U\", I");
        testMessage = new Message();
        testMessage.setEntityName(TEST_ENTITY);
        testMessage.setTags(tags);
        testMessage.setDate("2016-06-03T09:24:00.000Z");
        testMessage.setMessage("quo\"te");
    }

    @Test
    public void testComposing() {


        PlainCommand command = new MessageInsertCommand(
                testMessage.getEntityName(),
                testMessage.getTimestamp(),
                testMessage.getTags(),
                testMessage.getMessage()
        );


        assertEquals("Commands is composing incorrectly",
                String.format("message e:\"%s\" t:tag=\"OFF- RAMP \"\" U\"\", I\" m:\"quo\"\"te\"\n",
                        testMessage.getEntityName()
                ),
                command.compose()
        );
    }
}
