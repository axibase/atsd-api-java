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
import com.axibase.tsd.model.system.ServerError;
import com.axibase.tsd.query.QueryPart;
import com.fasterxml.jackson.jaxrs.base.JsonMappingExceptionMapper;
import com.fasterxml.jackson.jaxrs.base.JsonParseExceptionMapper;
import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;
import org.apache.commons.io.IOUtils;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.LayeredConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.AllowAllHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.StrictHostnameVerifier;
import org.apache.http.conn.ssl.X509HostnameVerifier;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.glassfish.jersey.SslConfigurator;
import org.glassfish.jersey.apache.connector.ApacheClientProperties;
import org.glassfish.jersey.apache.connector.ApacheConnectorProvider;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.glassfish.jersey.filter.LoggingFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.MessageBodyWriter;
import java.io.InputStream;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Collections;
import java.util.List;

import static com.axibase.tsd.util.AtsdUtil.JSON;


class HttpClient {
    private static final Logger log = LoggerFactory.getLogger(HttpClient.class);
    private final static java.util.logging.Logger legacyLogger = java.util.logging.Logger.getLogger(HttpClient.class.getName());
    public static final int HTTP_STATUS_OK = 200;
    public static final int HTTP_STATUS_FAIL = 400;
    public static final int HTTP_STATUS_NOT_FOUND = 404;

    private ClientConfiguration clientConfiguration;
    private final Client client;

    HttpClient(ClientConfiguration clientConfiguration) {
        client = buildClient(clientConfiguration);


        this.clientConfiguration = clientConfiguration;
    }

    private static Client buildClient(ClientConfiguration clientConfiguration) {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig
                .register(JsonMappingExceptionMapper.class)
                .register(JsonParseExceptionMapper.class)
                .register(JacksonJaxbJsonProvider.class, new Class[]{MessageBodyReader.class, MessageBodyWriter.class})
                .register(RequestBodyLogger.class)
                .register(HttpAuthenticationFeature.basic(clientConfiguration.getUsername(), clientConfiguration.getPassword()))
        ;

        if (log.isDebugEnabled()) {
            clientConfig.register(new LoggingFilter(legacyLogger, true));
        }

        configureHttps(clientConfiguration, clientConfig);

        clientConfig.connectorProvider(new ApacheConnectorProvider());

        Client builtClient = ClientBuilder.newBuilder().withConfig(clientConfig).build();
        builtClient.property(ClientProperties.CONNECT_TIMEOUT, clientConfiguration.getConnectTimeoutMillis());
        builtClient.property(ClientProperties.READ_TIMEOUT, clientConfiguration.getReadTimeoutMillis());
        return builtClient;
    }

    private static void configureHttps(ClientConfiguration clientConfiguration, ClientConfig clientConfig) {
        SslConfigurator sslConfig = SslConfigurator.newInstance().securityProtocol("SSL");
        PoolingHttpClientConnectionManager connectionManager = createConnectionManager(clientConfiguration, sslConfig);
        clientConfig.property(ApacheClientProperties.CONNECTION_MANAGER, connectionManager);
        clientConfig.property(ApacheClientProperties.SSL_CONFIG, sslConfig);
    }

    public static PoolingHttpClientConnectionManager createConnectionManager(ClientConfiguration clientConfiguration, SslConfigurator sslConfig) {
        SSLContext sslContext = sslConfig.createSSLContext();
        X509HostnameVerifier hostnameVerifier;
        if (clientConfiguration.isIgnoreSSLErrors()) {
            ignoreSslCertificateErrorInit(sslContext);
            hostnameVerifier = new AllowAllHostnameVerifier();
        } else {
            hostnameVerifier = new StrictHostnameVerifier();
        }

        LayeredConnectionSocketFactory sslSocketFactory = new SSLConnectionSocketFactory(
                sslContext,
                hostnameVerifier);

        final Registry<ConnectionSocketFactory> registry = RegistryBuilder.<ConnectionSocketFactory>create()
                .register("http", PlainConnectionSocketFactory.getSocketFactory())
                .register("https", sslSocketFactory)
                .build();
        return new PoolingHttpClientConnectionManager(registry);
    }

    private static void ignoreSslCertificateErrorInit(SSLContext sslContext) {
        try {
            sslContext.init(null, new TrustManager[]{
                    new AtsdTrustManager()
            }, new SecureRandom());
        } catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException e) {
            log.warn("SSL context initialization error: ", e);
        }
    }

    public <T> List<T> requestMetaDataList(Class<T> clazz, QueryPart<T> query) {
        return requestList(clientConfiguration.getMetadataUrl(), clazz, query, null);
    }

    public <T> T requestMetaDataObject(Class<T> clazz, QueryPart<T> query) {
        return requestObject(clientConfiguration.getMetadataUrl(), clazz, query, null);
    }

    public <E> boolean updateMetaData(QueryPart query, RequestProcessor<E> requestProcessor) {
        return update(clientConfiguration.getMetadataUrl(), query, requestProcessor);
    }

    public <E> boolean updateData(QueryPart query, RequestProcessor<E> requestProcessor) {
        return update(clientConfiguration.getDataUrl(), query, requestProcessor);
    }

    public boolean updateData(QueryPart query, String data) {
        return update(clientConfiguration.getDataUrl(), query, RequestProcessor.post(data), MediaType.TEXT_PLAIN);
    }

    public <T, E> List<T> requestDataList(Class<T> clazz, QueryPart<T> query, RequestProcessor<E> requestProcessor) {
        String url = clientConfiguration.getDataUrl();
        return requestList(url, clazz, query, requestProcessor);
    }

    public <T, E> T requestData(Class<T> clazz, QueryPart<T> query, RequestProcessor<E> requestProcessor) {
        String url = clientConfiguration.getDataUrl();
        return requestObject(url, clazz, query, requestProcessor);
    }

    private <T, E> List<T> requestList(String url, Class<T> resultClass, QueryPart<T> query, RequestProcessor<E> requestProcessor) {
        Response response = doRequest(url, query, requestProcessor);
        if (response.getStatus() == HTTP_STATUS_OK) {
            return response.readEntity(listType(resultClass));
        } else if (response.getStatus() == HTTP_STATUS_NOT_FOUND) {
            return Collections.emptyList();
        } else {
            throw buildException(response);
        }
    }

    private <T, E> T requestObject(String url, Class<T> resultClass, QueryPart<T> query, RequestProcessor<E> requestProcessor) {
        Response response = doRequest(url, query, requestProcessor);
        if (response.getStatus() == HTTP_STATUS_OK) {
            return response.readEntity(resultClass);
        } else if (response.getStatus() == HTTP_STATUS_NOT_FOUND) {
            buildAndLogServerError(response);
            return null;
        } else {
            throw buildException(response);
        }
    }

    public InputStream requestInputStream(QueryPart query, RequestProcessor requestProcessor) {
        String url = clientConfiguration.getDataUrl();
        Response response = doRequest(url, query, requestProcessor);
        Object entity = response.getEntity();
        if (response.getStatus() == HTTP_STATUS_OK && entity instanceof InputStream) {
            return (InputStream) entity;
        } else {
            throw buildException(response);
        }
    }

    private <E> boolean update(String url, QueryPart query, RequestProcessor<E> requestProcessor) {
        Response response = doRequest(url, query, requestProcessor);
        fixApacheHttpClientBlocking(response);
        if (response.getStatus() == HTTP_STATUS_OK) {
            return true;
        } else if (response.getStatus() == HTTP_STATUS_FAIL) {
            return false;
        } else {
            throw buildException(response);
        }
    }

    private <E> boolean update(String url, QueryPart query, RequestProcessor<E> requestProcessor, String mediaType) {
        Response response = doRequest(url, query, requestProcessor, mediaType);
        fixApacheHttpClientBlocking(response);
        if (response.getStatus() == HTTP_STATUS_OK) {
            return true;
        } else if (response.getStatus() == HTTP_STATUS_FAIL) {
            return false;
        } else {
            throw buildException(response);
        }
    }

    private AtsdServerException buildException(Response response) {
        ServerError serverError = buildAndLogServerError(response);
        return new AtsdServerException(response.getStatusInfo().getReasonPhrase() + " (" + response.getStatus() + ")" +
                ((serverError == null) ? "" : (", " + serverError.getMessage()))
        );
    }

    private ServerError buildAndLogServerError(Response response) {
        ServerError serverError = null;
        try {
            serverError = response.readEntity(ServerError.class);
            log.warn("Server error: {}", serverError);
        } catch (Throwable e) {
            log.warn("Couldn't read error message", e);
        }
        return serverError;
    }

    private <T, E> Response doRequest(String url, QueryPart<T> query, RequestProcessor<E> requestProcessor) {
        return doRequest(url, query, requestProcessor, JSON);
    }

    private <T, E> Response doRequest(String url, QueryPart<T> query, RequestProcessor<E> requestProcessor, String mediaType) {
        WebTarget target = client.target(url);
        target = query.fill(target);
        log.debug("url = {}", target.getUri());
        Invocation.Builder request = target.request(mediaType);

        Response response = null;
        try {
            if (requestProcessor == null) {
                response = request.get();
            } else {
                response = requestProcessor.process(request, mediaType);
            }
        } catch (Throwable e) {
            throw new AtsdClientException("Error while processing the request", e);
        }
        return response;
    }

    private <T> GenericType<List<T>> listType(final Class<T> clazz) {
        ParameterizedType genericType = new ParameterizedType() {
            public Type[] getActualTypeArguments() {
                return new Type[]{clazz};
            }

            public Type getRawType() {
                return List.class;
            }

            public Type getOwnerType() {
                return List.class;
            }
        };
        return new GenericType<List<T>>(genericType) {
        };
    }

    public void close() {
        if (client != null) {
            client.close();
        }
    }

    private static void fixApacheHttpClientBlocking(Response response) {
        Object entity = response.getEntity();
        if (entity instanceof InputStream) {
            IOUtils.closeQuietly((InputStream) entity);
        }
    }
}
