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

import com.axibase.tsd.model.system.ClientConfiguration;
import com.axibase.tsd.network.MarkerCommand;
import com.axibase.tsd.network.PlainCommand;
import com.axibase.tsd.util.AtsdUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.entity.AbstractHttpEntity;
import org.apache.http.entity.BufferedHttpEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.glassfish.jersey.SslConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.DatatypeConverter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static com.axibase.tsd.util.AtsdUtil.MARKER_KEYWORD;


class PlainStreamingSender extends AbstractHttpEntity implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(PlainStreamingSender.class);
    private static final int SMALL = 64;

    private String url;
    private CountDownLatch latch = new CountDownLatch(1);
    private CloseableHttpClient httpClient;
    private BlockingQueue<String> messages;
    private ConcurrentMap<String, List<String>> markerToMessages = new ConcurrentHashMap<>();
    private volatile SenderState state = SenderState.NEW;
    private final long pingTimeoutMillis;
    private long lastMessageTime;
    private CloseableHttpResponse response;
    private final ClientConfiguration clientConfiguration;
    private PoolingHttpClientConnectionManager connectionManager;

    public PlainStreamingSender(ClientConfiguration clientConfiguration, PlainStreamingSender old) {
        this.clientConfiguration = clientConfiguration;
        this.url = clientConfiguration.getDataUrl();
        this.pingTimeoutMillis = clientConfiguration.getPingTimeoutMillis();
        if (old != null) {
            messages = old.messages;
            markerToMessages = old.markerToMessages;
            log.info("Reborn network commands sender using previous messages, size: {}", messages.size());
        }
    }

    public void send(PlainCommand plainCommand) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            log.error("Initialization error:", e);
        }

        if (state != SenderState.WORKING) {
            throw new IllegalStateException("Could not send command using incorrect sender");
        }

        String text = plainCommand.compose();
        if (StringUtils.isBlank(text)) {
            log.error("Command is empty");
            return;
        }

        if (!text.endsWith("\n")) {
            text = text + "\n";
        }
        messages.add(text);
        log.debug("Message is added to queue, queue size = {}", messages.size());
    }

    @Override
    public boolean isRepeatable() {
        return false;
    }

    @Override
    public boolean isChunked() {
        return false;
    }

    @Override
    public long getContentLength() {
        return -1;
    }

    @Override
    public InputStream getContent() throws IOException, IllegalStateException {
        return null;
    }

    @Override
    public void writeTo(OutputStream outputStream) throws IOException {
        String marker = null;
        while (state == SenderState.WORKING) {
            String message = null;
            try {
                message = messages.poll(pingTimeoutMillis, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                if (state == SenderState.WORKING) {
                    log.error("Could not poll message from queue", e);
                }
            }

            try {
                if (message != null) {
                    if (!clientConfiguration.isSkipStreamingControl() &&
                            marker == null && !message.startsWith(MARKER_KEYWORD)
                            ) {
                        MarkerCommand markerCommand = new MarkerCommand();
                        marker = markerCommand.getMarker();
                        write(outputStream, markerCommand.compose());
                    }

                    log.debug("Write message: {}", message);
                    write(outputStream, message);

                    if (!clientConfiguration.isSkipStreamingControl()) {
                        if (message.startsWith(MARKER_KEYWORD)) {
                            marker = StringUtils.removeStart(message, MARKER_KEYWORD).trim();
                            if (StringUtils.isBlank(marker)) {
                                throw new IllegalArgumentException("Bad marker message: " + message);
                            }
                        } else {
                            add(marker, message);
                        }
                    }

                    lastMessageTime = System.currentTimeMillis();
                }
            } catch (IOException e) {
                log.error("Sender is broken, close it. Could not send message: {}", message, e);
                messages.add(message);
                close();
                return;
            }
            if (lastMessageTime - System.currentTimeMillis() > pingTimeoutMillis) {
                write(outputStream, AtsdUtil.PING_COMMAND);
                if (!clientConfiguration.isSkipStreamingControl()) {
                    add(marker, AtsdUtil.PING_COMMAND);
                }
                lastMessageTime = System.currentTimeMillis();
            }
        }
    }

    private void write(OutputStream outputStream, String text) throws IOException {
        outputStream.write(text.getBytes());
        outputStream.flush();
    }

    private void add(String marker, String message) {
        if (clientConfiguration.isSkipStreamingControl()) {
            log.error("Could not add message to marker, because streaming control is skipped, marker = {}, message= {}",
                    marker, message);
            throw new IllegalStateException("Could not add message to marker during skipped streaming control");
        }

        List<String> stored = markerToMessages.get(marker);
        if (stored == null) {
            stored = new ArrayList<String>();
            final List<String> prev = markerToMessages.putIfAbsent(marker, stored);
            stored = prev == null ? stored : prev;
        }
        stored.add(message);
    }

    @Override
    public boolean isStreaming() {
        return true;
    }

    @Override
    public void run() {
        if (messages == null) {
            messages = new LinkedBlockingQueue<String>();
        }
        HttpPost httpPost = null;
        try {
            SslConfigurator sslConfig = SslConfigurator.newInstance().securityProtocol("SSL");
            connectionManager = HttpClient.createConnectionManager(clientConfiguration, sslConfig);
            connectionManager.setDefaultConnectionConfig(ConnectionConfig.custom().setBufferSize(SMALL).build());
            httpClient = HttpClients.custom()
                    .setConnectionManager(connectionManager)
                    .build();
            httpPost = new HttpPost(fullUrl());
            httpPost.setHeader("Authorization", "Basic " + DatatypeConverter.printBase64Binary(
                    (clientConfiguration.getUsername() + ":" + clientConfiguration.getPassword()).getBytes()
            ));
            httpPost.setEntity(new BufferedHttpEntity(this));
        } catch (IOException e) {
            log.error("Could not create http client: ", e);
            latch.countDown();
            close();
            return;
        }
        try {
            log.info("Start writing commands to {}", fullUrl());
            state = SenderState.WORKING;
            latch.countDown();
            response = httpClient.execute(httpPost);
        } catch (IOException e) {
            log.error("Could not execute HTTP POST: {}", httpPost, e);
        } finally {
            log.info("Http post execution is finished, close sender");
            close();
        }
    }

    public void close() {
        if (state == SenderState.CLOSED) {
            return;
        }
        log.info("Stop writing commands to {}", fullUrl());
        if (response != null) {
            try {
                response.close();
            } catch (IOException e) {
                log.error("Could not close response: {}", response, e);
            }
        }
        if (httpClient != null) {
            try {
                httpClient.close();
            } catch (IOException e) {
                log.error("Could not close client: {}", httpClient, e);
            }
        }
        if (connectionManager != null) {
            connectionManager.close();
        }
        state = SenderState.CLOSED;
    }

    private String fullUrl() {
        return url + "/command";
    }

    public boolean isWorking() {
        return state == SenderState.WORKING;
    }

    public boolean isClosed() {
        return state == SenderState.CLOSED;
    }

    public Map<String, List<String>> getMarkerToMessages() {
        return markerToMessages;
    }

    private enum SenderState {
        NEW,
        WORKING,
        CLOSED
    }
}
