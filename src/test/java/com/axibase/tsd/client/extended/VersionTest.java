package com.axibase.tsd.client.extended;

import com.axibase.tsd.TestUtil;
import com.axibase.tsd.client.ExtendedService;
import com.axibase.tsd.client.ServiceFactory;
import com.axibase.tsd.model.version.AtsdVersion;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;

@Slf4j
public class VersionTest {
    private ExtendedService extendedService = ServiceFactory
            .with(TestUtil.buildHttpClientManager())
            .extended();

    @Test
    public void testVersion() {
        final AtsdVersion version = extendedService.version();
        Assert.assertNotNull(version);
        log.info(version.toString());
        Assert.assertNotNull(version.getBuildInfo());
        Assert.assertNotNull(version.getLicense());
        Assert.assertNotNull(version.getDate());
        Assert.assertNotNull(version.getSettings());
    }
}
