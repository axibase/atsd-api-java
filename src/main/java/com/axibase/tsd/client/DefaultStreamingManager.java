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

import com.axibase.tsd.model.system.MarkerState;
import com.axibase.tsd.network.MarkerCommand;
import com.axibase.tsd.network.PlainCommand;
import com.axibase.tsd.query.Query;
import com.axibase.tsd.query.QueryPart;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class DefaultStreamingManager implements StreamingManager {
    private static final Logger log = LoggerFactory.getLogger(DefaultStreamingManager.class);
    public static final String CHECK = "check";
    private static final int DEFAULT_CHECK_PERIOD_MS = 5000;
    private long checkPeriodMillis = DEFAULT_CHECK_PERIOD_MS;
    private PlainStreamingSender plainSender = null;
    private final AtomicLong lastPingTime = new AtomicLong(0);
    private final AtomicReference<String> marker = new AtomicReference<String>();
    private boolean lastPingResult = false;
    private final List<String> saved = new ArrayList<String>();
    private final HttpClientManager httpClientManager;
    private Future<?> senderFuture;
    private ExecutorService checkExecutor;
    private ExecutorService senderExecutor;
    private ReadWriteLock senderLock = new ReentrantReadWriteLock();

    public DefaultStreamingManager(HttpClientManager httpClientManager) {
        if (httpClientManager == null) {
            throw new IllegalArgumentException("httpClientManager is null");
        }
        this.httpClientManager = httpClientManager;
        checkExecutor = Executors.newSingleThreadExecutor();
        senderExecutor = Executors.newSingleThreadExecutor();
    }

    @Override
    public void setCheckPeriodMillis(long checkPeriodMillis) {
        this.checkPeriodMillis = checkPeriodMillis;
    }

    @Override
    public void close() {
        log.info("Close streaming manager");
        PlainStreamingSender sender = plainSender;
        if (sender != null) {
            sender.close();
        }
        checkExecutor.shutdown();
        senderExecutor.shutdown();
    }

    @Override
    public void send(PlainCommand plainCommand) {
        if (!lastPingResult) {
            throw new IllegalStateException("Last check was bad, call canSend() method before command sending");
        }
        Lock readLock = senderLock.readLock();
        readLock.lock();
        try {
            PlainStreamingSender sender = plainSender;
            if (sender == null) {
                throw new IllegalStateException("Sender is null");
            } else if (!sender.isWorking()) {
                throw new IllegalStateException("Sender is in the wrong state");
            }
            sender.send(plainCommand);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public boolean canSend() {
        for (; ; ) {
            long last = lastPingTime.get();
            long current = System.currentTimeMillis();
            if (current - last > checkPeriodMillis) {
                if (lastPingTime.compareAndSet(last, current)) {
                    checkExecutor.execute(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                boolean beforeLastResult = lastPingResult;
                                prepareAndCheckSender();
                                if (beforeLastResult && lastPingResult) {
                                    saved.clear();
                                }
                            } catch (Throwable e) {
                                log.error("Could not prepare sender: ", e);
                            }
                        }
                    });
                    break;
                }
            } else {
                break;
            }
        }
        Lock readLock = senderLock.readLock();
        readLock.lock();
        try {
            return lastPingResult && plainSender != null && plainSender.isWorking();
        } finally {
            readLock.unlock();
        }
    }

    private void prepareAndCheckSender() {
        if (plainSender == null || plainSender.isClosed()) {
            Lock writeLock = senderLock.writeLock();
            writeLock.lock();

            try {
                if (plainSender == null || plainSender.isClosed()) {

                    PlainStreamingSender newSender = new PlainStreamingSender(httpClientManager.getClientConfiguration(), plainSender);
                    if (plainSender != null) {
                        log.info("Prepare new sender, close old");
                        plainSender.close();
                    }
                    if (senderFuture != null) {
                        senderFuture.cancel(true);
                    }
                    senderFuture = senderExecutor.submit(newSender);
                    plainSender = newSender;
                }
            } finally {
                writeLock.unlock();
            }
        }
        lastPingResult = check();
        if (lastPingResult) {
            compareAndSendNewMarker(marker.get());
        }
    }

    private boolean check() {
        if (httpClientManager.getClientConfiguration().isSkipStreamingControl()) {
            return true;
        }
        boolean needClosing = false;
        Lock readLock = senderLock.readLock();
        readLock.lock();
        try {

            if (plainSender != null) {
                Map<String, List<String>> markerToMessages = plainSender.getMarkerToMessages();
                int size = markerToMessages.size();
                if (size <= 2) {
                    // just check
                    MarkerState markerState = askMarkerState(CHECK);
                    boolean checkResult = markerState != null && CHECK.equals(markerState.getMarker());
                    if (!checkResult) {
                        log.warn("Bad check result, close sender");
                        needClosing = true;
                    }
                    return checkResult;
                }

                int i = 0;

                for (Iterator<Map.Entry<String, List<String>>> iterator = markerToMessages.entrySet().iterator();
                     iterator.hasNext() && i < size - 2; i++) {
                    Map.Entry<String, List<String>> markerAndCommands = iterator.next();
                    String checkedMarker = markerAndCommands.getKey();
                    MarkerState markerState = askMarkerState(checkedMarker);
                    List<String> commands = markerAndCommands.getValue();
                    if (markerState != null && markerState.getCount() != null) {
                        if (markerState.getCount() > commands.size()) {
                            log.warn("Server received more ({}) commands then client sent ({}), marker: {}",
                                    markerState.getCount(), commands.size(), checkedMarker);
                        } else if (markerState.getCount() < commands.size()) {
                            log.error("Server received less ({}) commands then client sent ({}), marker: {}",
                                    markerState.getCount(), commands.size(), checkedMarker);
                            saved.addAll(commands);
                        } else {
                            log.debug("Server received same command count ({}) that client sent, marker: {}",
                                    commands.size(), checkedMarker);
                        }
                        iterator.remove();
                    } else {
                        log.warn("Could not get command count for marker {}", marker);
                        saved.addAll(commands);
                        iterator.remove();
                    }
                }

                if (saved.isEmpty()) {
                    return true;
                } else {
                    for (Iterator<Map.Entry<String, List<String>>> iterator = markerToMessages.entrySet().iterator();
                         iterator.hasNext(); ) {
                        Map.Entry<String, List<String>> markerAndCommands = iterator.next();
                        List<String> commands = markerAndCommands.getValue();
                        saved.addAll(commands);
                        iterator.remove();
                    }
                    log.warn("Save {} commands, broken sender will be closed", saved.size());
                    needClosing = true;
                    return false;
                }
            } else {
                log.warn("Sender is null");
                return false;
            }
        } catch (Throwable e) {
            log.warn("Ping error: ", e);
            return false;
        } finally {
            readLock.unlock();
            if (needClosing) {
                Lock writeLock = senderLock.writeLock();
                writeLock.lock();
                try {
                    plainSender.close();
                } finally {
                    writeLock.unlock();
                }
            }
        }
    }

    private MarkerState askMarkerState(String marker) {
        MarkerState markerState = null;
        try {
            QueryPart<MarkerState> markersPath = new Query<MarkerState>("command").path("marker");
            QueryPart<MarkerState> query = markersPath.param("v", marker);
            markerState = httpClientManager.requestData(MarkerState.class, query, null);
            log.debug("From server {} received the following state of marker ({}): {}",
                    httpClientManager.getClientConfiguration().getDataUrl(), marker, markerState);
        } catch (Throwable e) {
            log.error("Error while checking marker count: ", e);
        }
        return markerState;
    }

    private void compareAndSendNewMarker(String current) {
        log.debug("Send merker: {}", current);

        if (httpClientManager.getClientConfiguration().isSkipStreamingControl()) {
            return;
        }

        MarkerCommand markerCommand = new MarkerCommand();
        final String newMarker = markerCommand.getMarker();

        if (marker.compareAndSet(current, newMarker)) {
            Lock readLock = senderLock.readLock();
            readLock.lock();
            try {
                if (plainSender == null) {
                    throw new IllegalStateException("Sender is null");
                } else if (!plainSender.isWorking()) {
                    throw new IllegalStateException("Sender is incorrect");
                } else {
                    plainSender.send(markerCommand);
                }
            } finally {
                readLock.unlock();
            }
        } else {
            log.warn("Current marker:{} is already replaced by another marker:", current, marker.get());
        }
    }

    @Override
    public List<String> removeSavedPlainCommands() {
        if (saved.isEmpty()) {
            return Collections.emptyList();
        }
        synchronized (saved) {
            List<String> result = new ArrayList<String>(saved);
            saved.removeAll(result);
            if (result.size() > 0) {
                log.info("{} commands are removed from saved list", result.size());
            }
            return result;
        }
    }

}
