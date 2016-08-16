package com.axibase.tsd.client.command;

import com.axibase.tsd.TestUtil;
import com.axibase.tsd.client.DataService;
import com.axibase.tsd.client.DefaultStreamingManager;
import com.axibase.tsd.client.HttpClientManager;
import com.axibase.tsd.model.data.command.GetSeriesQuery;
import com.axibase.tsd.model.data.series.Sample;
import com.axibase.tsd.model.data.series.Series;
import com.axibase.tsd.network.InsertCommand;
import com.axibase.tsd.network.PlainCommand;
import com.axibase.tsd.util.AtsdUtil;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * @author Igor Shmagrinskiy
 */
public class CommandSeriesQuotesTest {
    private final static String TEST_PREFIX = "command-series-quotes-test-";
    private final static String TEST_ENTITY = TEST_PREFIX + "entity";
    private final static String TEST_METRIC = TEST_PREFIX + "metric";
    private static HttpClientManager httpClientManager = TestUtil.buildHttpClientManager();
    private static DefaultStreamingManager manager = new DefaultStreamingManager(httpClientManager);
    private static DataService dataService = new DataService(httpClientManager);
    private static Series testSeries;

    @BeforeClass
    public static void prepareData() {
        Sample testSample = new Sample(TestUtil.parseDate("2016-06-03T09:24:00.000Z").getTime(), -31.1);
        Map<String, String> tags = Collections.singletonMap("tag", "OFF- RAMP \" U\", I");
        testSeries = new Series();
        testSeries.setEntityName(TEST_ENTITY);
        testSeries.setMetricName(TEST_METRIC);
        testSeries.setData(Collections.singletonList(testSample));
        testSeries.setTags(tags);
    }

    @Test
    public void testComposing() {


        PlainCommand command = new InsertCommand(
                testSeries.getEntityName(),
                testSeries.getMetricName(),
                testSeries.getData().get(0),
                testSeries.getTags()
        );

        Sample testSample = testSeries.getData().get(0);

        assertEquals("Commands is composing incorrectly",
                String.format("series e:%s ms:%d t:tag=\"OFF- RAMP \"\" U\"\", I\" m:%s=%s\n",
                        TEST_ENTITY, testSample.getTimeMillis(), TEST_METRIC, testSample.getValue()),
                command.compose()
        );
    }


    @Test
    public void testInserting() {
        PlainCommand command = new InsertCommand(
                testSeries.getEntityName(),
                testSeries.getMetricName(),
                testSeries.getData().get(0),
                testSeries.getTags()
        );
        if (manager.canSend()) {
            System.out.println(command.compose());
            manager.send(command);
        }

        GetSeriesQuery getSeriesQuery = new GetSeriesQuery(testSeries.getEntityName(), testSeries.getMetricName(), testSeries.getTags());
        getSeriesQuery.setStartDate(AtsdUtil.DateTime.MIN_QUERIED_DATE_TIME);
        getSeriesQuery.setEndDate(AtsdUtil.DateTime.MAX_QUERIED_DATE_TIME);
        List<Series> retrievedSeriesList = dataService.retrieveSeries(getSeriesQuery);

        assertEquals(retrievedSeriesList.get(0), testSeries);
    }
}
