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
package com.axibase.tsd.example;

import com.axibase.tsd.client.SeriesCommandPreparer;
import com.axibase.tsd.model.data.command.AddSeriesCommand;
import com.axibase.tsd.model.data.command.GetSeriesQuery;
import com.axibase.tsd.model.data.series.Series;
import com.axibase.tsd.util.AtsdUtil;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class AtsdClientWriteExample extends AbstractAtsdClientExample {
    public static final long SECOND = 1000L;
    public static final int MB = 1024 * 1024;
    public static final int CNT = 10;
    private static final double MAX_VALUE = 1517191;
    private String hostName;

    public static void main(String[] args) {
        AtsdClientWriteExample atsdClientWriteExample = new AtsdClientWriteExample();
        atsdClientWriteExample.configure();
        atsdClientWriteExample.writeData();
        atsdClientWriteExample.printData();
    }

    private void sendToAtsd(double totalMemoryMb, double freeMemoryMb) {
        hostName = "localhost";
        try {
            hostName = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        long time = System.currentTimeMillis();
        dataService.addSeries(
                AddSeriesCommand.createSingle(hostName, "total_memory_mb", time, totalMemoryMb, "app_name", "atsd_writer_example"),
                AddSeriesCommand.createSingle(hostName, "free_memory_mb", time, freeMemoryMb, "app_name", "atsd_writer_example")
        );
    }

    protected void printData() {
        Map<String, String> tags = AtsdUtil.toMap("app_name", "atsd_writer_example");
        List<Series> series = dataService.retrieveSeries(
                new SeriesCommandPreparer() {
                    @Override
                    public void prepare(GetSeriesQuery command) {
                        command.setLimit(CNT);
                        command.setStartTime(System.currentTimeMillis() - (CNT + 2) * 1000L);
                        command.setEndTime(System.currentTimeMillis());
                    }
                },
                new GetSeriesQuery(hostName, "total_memory_mb", tags),
                new GetSeriesQuery(hostName, "free_memory_mb", tags)
        );
        logger.info("===Sample===");
        for (Series s : series) {
            print(s);
        }
    }

    protected void writeData() {
        logger.info("Writing memory usage metrics to ATSD ...");
        Runtime runtime = Runtime.getRuntime();
        Set<String> memoryEater = new HashSet<String>();
        for (int i = 0; i < CNT; i++) {
            long st = System.currentTimeMillis();
            StringBuilder sb = new StringBuilder();
            for (int k = 0; k < MAX_VALUE * ((i + 0D) / CNT); k++) {
                sb.append(String.valueOf(k));
            }
            memoryEater.add(sb.toString());

            long totalMemoryMb = runtime.totalMemory() / MB;
            long freeMemoryMb = runtime.freeMemory() / MB;
            sendToAtsd(totalMemoryMb, freeMemoryMb);

            try {
                long delta = System.currentTimeMillis() - st;
                if (delta < SECOND) {
                    Thread.sleep(SECOND - delta);
                }
            } catch (InterruptedException e) {
                // ignore
                e.printStackTrace();
            }

            System.out.print(i + " ");
        }
        logger.info("\n");
    }
}
