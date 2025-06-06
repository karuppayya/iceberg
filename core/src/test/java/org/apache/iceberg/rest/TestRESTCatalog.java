/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.rest;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.map;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTransaction;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdatePartitionSpec;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.catalog.CatalogTests;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.TableCommit;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.exceptions.ServiceFailureException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.HTTPRequest.HTTPMethod;
import org.apache.iceberg.rest.RESTSessionCatalog.SnapshotMode;
import org.apache.iceberg.rest.auth.AuthSessionUtil;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.iceberg.rest.auth.OAuth2Util;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.CreateNamespaceResponse;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.OAuthTokenResponse;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.awaitility.Awaitility;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.gzip.GzipHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

public class TestRESTCatalog extends CatalogTests<RESTCatalog> {
  private static final ObjectMapper MAPPER = RESTObjectMapper.mapper();
  private static final ResourcePaths RESOURCE_PATHS =
      ResourcePaths.forCatalogProperties(Maps.newHashMap());

  @TempDir public Path temp;

  private RESTCatalog restCatalog;
  private InMemoryCatalog backendCatalog;
  private Server httpServer;

  @BeforeEach
  public void createCatalog() throws Exception {
    File warehouse = temp.toFile();

    this.backendCatalog = new InMemoryCatalog();
    this.backendCatalog.initialize(
        "in-memory",
        ImmutableMap.of(CatalogProperties.WAREHOUSE_LOCATION, warehouse.getAbsolutePath()));

    HTTPHeaders catalogHeaders =
        HTTPHeaders.of(
            Map.of(
                "Authorization",
                "Bearer client-credentials-token:sub=catalog",
                "test-header",
                "test-value"));
    HTTPHeaders contextHeaders =
        HTTPHeaders.of(
            Map.of(
                "Authorization",
                "Bearer client-credentials-token:sub=user",
                "test-header",
                "test-value"));

    RESTCatalogAdapter adaptor =
        new RESTCatalogAdapter(backendCatalog) {
          @Override
          public <T extends RESTResponse> T execute(
              HTTPRequest request,
              Class<T> responseType,
              Consumer<ErrorResponse> errorHandler,
              Consumer<Map<String, String>> responseHeaders) {
            // this doesn't use a Mockito spy because this is used for catalog tests, which have
            // different method calls
            if (!"v1/oauth/tokens".equals(request.path())) {
              if ("v1/config".equals(request.path())) {
                assertThat(request.headers().entries()).containsAll(catalogHeaders.entries());
              } else {
                assertThat(request.headers().entries()).containsAll(contextHeaders.entries());
              }
            }
            Object body = roundTripSerialize(request.body(), "request");
            HTTPRequest req = ImmutableHTTPRequest.builder().from(request).body(body).build();
            T response = super.execute(req, responseType, errorHandler, responseHeaders);
            T responseAfterSerialization = roundTripSerialize(response, "response");
            return responseAfterSerialization;
          }
        };

    ServletContextHandler servletContext =
        new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
    servletContext.addServlet(new ServletHolder(new RESTCatalogServlet(adaptor)), "/*");
    servletContext.setHandler(new GzipHandler());

    this.httpServer = new Server(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
    httpServer.setHandler(servletContext);
    httpServer.start();

    this.restCatalog = initCatalog("prod", ImmutableMap.of());
  }

  @Override
  protected RESTCatalog initCatalog(String catalogName, Map<String, String> additionalProperties) {
    Configuration conf = new Configuration();
    SessionCatalog.SessionContext context =
        new SessionCatalog.SessionContext(
            UUID.randomUUID().toString(),
            "user",
            ImmutableMap.of("credential", "user:12345"),
            ImmutableMap.of());

    RESTCatalog catalog =
        new RESTCatalog(
            context,
            (config) ->
                HTTPClient.builder(config)
                    .uri(config.get(CatalogProperties.URI))
                    .withHeaders(RESTUtil.configHeaders(config))
                    .build());
    catalog.setConf(conf);
    Map<String, String> properties =
        ImmutableMap.of(
            CatalogProperties.URI,
            httpServer.getURI().toString(),
            CatalogProperties.FILE_IO_IMPL,
            "org.apache.iceberg.inmemory.InMemoryFileIO",
            CatalogProperties.TABLE_DEFAULT_PREFIX + "default-key1",
            "catalog-default-key1",
            CatalogProperties.TABLE_DEFAULT_PREFIX + "default-key2",
            "catalog-default-key2",
            CatalogProperties.TABLE_DEFAULT_PREFIX + "override-key3",
            "catalog-default-key3",
            CatalogProperties.TABLE_OVERRIDE_PREFIX + "override-key3",
            "catalog-override-key3",
            CatalogProperties.TABLE_OVERRIDE_PREFIX + "override-key4",
            "catalog-override-key4",
            "credential",
            "catalog:12345",
            "header.test-header",
            "test-value");
    catalog.initialize(
        catalogName,
        ImmutableMap.<String, String>builder()
            .putAll(properties)
            .putAll(additionalProperties)
            .build());
    return catalog;
  }

  @SuppressWarnings("unchecked")
  public static <T> T roundTripSerialize(T payload, String description) {
    if (payload != null) {
      try {
        if (payload instanceof RESTMessage) {
          return (T) MAPPER.readValue(MAPPER.writeValueAsString(payload), payload.getClass());
        } else {
          // use Map so that Jackson doesn't try to instantiate ImmutableMap from payload.getClass()
          return (T) MAPPER.readValue(MAPPER.writeValueAsString(payload), Map.class);
        }
      } catch (JsonProcessingException e) {
        throw new RuntimeException(
            String.format("Failed to serialize and deserialize %s: %s", description, payload), e);
      }
    }
    return null;
  }

  @AfterEach
  public void closeCatalog() throws Exception {
    if (restCatalog != null) {
      restCatalog.close();
    }

    if (backendCatalog != null) {
      backendCatalog.close();
    }

    if (httpServer != null) {
      httpServer.stop();
      httpServer.join();
    }
  }

  @Override
  protected RESTCatalog catalog() {
    return restCatalog;
  }

  @Override
  protected boolean supportsNamespaceProperties() {
    return true;
  }

  @Override
  protected boolean supportsServerSideRetry() {
    return true;
  }

  @Override
  protected boolean supportsNestedNamespaces() {
    return true;
  }

  @Override
  protected boolean requiresNamespaceCreate() {
    return true;
  }

  /* RESTCatalog specific tests */

  @Test
  public void testConfigRoute() throws IOException {
    RESTClient testClient =
        new RESTCatalogAdapter(backendCatalog) {
          @Override
          public <T extends RESTResponse> T execute(
              HTTPRequest request,
              Class<T> responseType,
              Consumer<ErrorResponse> errorHandler,
              Consumer<Map<String, String>> responseHeaders) {
            if ("v1/config".equals(request.path())) {
              return castResponse(
                  responseType,
                  ConfigResponse.builder()
                      .withDefaults(ImmutableMap.of(CatalogProperties.CLIENT_POOL_SIZE, "1"))
                      .withOverrides(
                          ImmutableMap.of(
                              CatalogProperties.CACHE_ENABLED,
                              "false",
                              CatalogProperties.WAREHOUSE_LOCATION,
                              request.queryParameters().get(CatalogProperties.WAREHOUSE_LOCATION)
                                  + "warehouse"))
                      .build());
            }
            return super.execute(request, responseType, errorHandler, responseHeaders);
          }
        };

    RESTCatalog restCat = new RESTCatalog((config) -> testClient);
    Map<String, String> initialConfig =
        ImmutableMap.of(
            CatalogProperties.URI, "http://localhost:8080",
            CatalogProperties.CACHE_ENABLED, "true",
            CatalogProperties.WAREHOUSE_LOCATION, "s3://bucket/");

    restCat.setConf(new Configuration());
    restCat.initialize("prod", initialConfig);

    assertThat(restCat.properties().get(CatalogProperties.CACHE_ENABLED))
        .as("Catalog properties after initialize should use the server's override properties")
        .isEqualTo("false");

    assertThat(restCat.properties().get(CatalogProperties.CLIENT_POOL_SIZE))
        .as("Catalog after initialize should use the server's default properties if not specified")
        .isEqualTo("1");

    assertThat(restCat.properties().get(CatalogProperties.WAREHOUSE_LOCATION))
        .as("Catalog should return final warehouse location")
        .isEqualTo("s3://bucket/warehouse");

    restCat.close();
  }

  @Test
  public void testInitializeWithBadArguments() throws IOException {
    RESTCatalog restCat = new RESTCatalog();
    assertThatThrownBy(() -> restCat.initialize("prod", null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid configuration: null");

    assertThatThrownBy(() -> restCat.initialize("prod", ImmutableMap.of()))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid uri for http client: null");

    restCat.close();
  }

  @Test
  public void testDefaultHeadersPropagated() {
    RESTCatalog catalog = new RESTCatalog();
    Map<String, String> properties =
        Map.of(
            CatalogProperties.URI,
            httpServer.getURI().toString(),
            OAuth2Properties.CREDENTIAL,
            "catalog:secret",
            "header.test-header",
            "test-value",
            "header.test-header2",
            "test-value2");
    catalog.initialize("test", properties);
    assertThat(catalog)
        .extracting("sessionCatalog.client.baseHeaders")
        .asInstanceOf(map(String.class, String.class))
        .containsEntry("test-header2", "test-value2");
  }

  @Test
  public void testCatalogBasicBearerToken() {
    Map<String, String> catalogHeaders = ImmutableMap.of("Authorization", "Bearer bearer-token");

    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));

    RESTCatalog catalog =
        new RESTCatalog(SessionCatalog.SessionContext.createEmpty(), (config) -> adapter);
    catalog.initialize(
        "prod", ImmutableMap.of(CatalogProperties.URI, "ignored", "token", "bearer-token"));

    assertThat(catalog.tableExists(TBL)).isFalse();

    // the bearer token should be used for all interactions
    Mockito.verify(adapter)
        .execute(
            reqMatcher(HTTPMethod.GET, "v1/config", catalogHeaders),
            eq(ConfigResponse.class),
            any(),
            any());
    Mockito.verify(adapter)
        .execute(
            reqMatcher(HTTPMethod.HEAD, RESOURCE_PATHS.table(TBL), catalogHeaders),
            any(),
            any(),
            any());
  }

  @Test
  public void testCatalogCredentialNoOauth2ServerUri() {
    Map<String, String> emptyHeaders = ImmutableMap.of();
    Map<String, String> catalogHeaders =
        ImmutableMap.of("Authorization", "Bearer client-credentials-token:sub=catalog");

    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));

    RESTCatalog catalog =
        new RESTCatalog(SessionCatalog.SessionContext.createEmpty(), (config) -> adapter);
    catalog.initialize(
        "prod", ImmutableMap.of(CatalogProperties.URI, "ignored", "credential", "catalog:secret"));

    assertThat(catalog.tableExists(TBL)).isFalse();

    // no token or credential for catalog token exchange
    Mockito.verify(adapter)
        .execute(
            reqMatcher(HTTPMethod.POST, "v1/oauth/tokens", emptyHeaders),
            eq(OAuthTokenResponse.class),
            any(),
            any());
    // no token or credential for config
    Mockito.verify(adapter)
        .execute(
            reqMatcher(HTTPMethod.GET, "v1/config", catalogHeaders),
            eq(ConfigResponse.class),
            any(),
            any());
    // use the catalog token for all interactions
    Mockito.verify(adapter)
        .execute(
            reqMatcher(HTTPMethod.HEAD, RESOURCE_PATHS.table(TBL), catalogHeaders),
            any(),
            any(),
            any());
  }

  @ParameterizedTest
  @ValueSource(strings = {"v1/oauth/tokens", "https://auth-server.com/token"})
  public void testCatalogCredential(String oauth2ServerUri) {
    Map<String, String> emptyHeaders = ImmutableMap.of();
    Map<String, String> catalogHeaders =
        ImmutableMap.of("Authorization", "Bearer client-credentials-token:sub=catalog");

    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));

    RESTCatalog catalog =
        new RESTCatalog(SessionCatalog.SessionContext.createEmpty(), (config) -> adapter);
    catalog.initialize(
        "prod",
        ImmutableMap.of(
            CatalogProperties.URI,
            "ignored",
            "credential",
            "catalog:secret",
            OAuth2Properties.OAUTH2_SERVER_URI,
            oauth2ServerUri));

    assertThat(catalog.tableExists(TBL)).isFalse();

    // no token or credential for catalog token exchange
    Mockito.verify(adapter)
        .execute(
            reqMatcher(HTTPMethod.POST, oauth2ServerUri, emptyHeaders),
            eq(OAuthTokenResponse.class),
            any(),
            any());
    // no token or credential for config
    Mockito.verify(adapter)
        .execute(
            reqMatcher(HTTPMethod.GET, "v1/config", catalogHeaders),
            eq(ConfigResponse.class),
            any(),
            any());
    // use the catalog token for all interactions
    Mockito.verify(adapter)
        .execute(
            reqMatcher(HTTPMethod.HEAD, RESOURCE_PATHS.table(TBL), catalogHeaders),
            any(),
            any(),
            any());
  }

  @ParameterizedTest
  @ValueSource(strings = {"v1/oauth/tokens", "https://auth-server.com/token"})
  public void testCatalogBearerTokenWithClientCredential(String oauth2ServerUri) {
    Map<String, String> contextHeaders =
        ImmutableMap.of("Authorization", "Bearer client-credentials-token:sub=user");
    Map<String, String> catalogHeaders = ImmutableMap.of("Authorization", "Bearer bearer-token");

    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));

    SessionCatalog.SessionContext context =
        new SessionCatalog.SessionContext(
            UUID.randomUUID().toString(),
            "user",
            ImmutableMap.of("credential", "user:secret"),
            ImmutableMap.of());

    RESTCatalog catalog = new RESTCatalog(context, (config) -> adapter);
    catalog.initialize(
        "prod",
        ImmutableMap.of(
            CatalogProperties.URI,
            "ignored",
            "token",
            "bearer-token",
            OAuth2Properties.OAUTH2_SERVER_URI,
            oauth2ServerUri));

    assertThat(catalog.tableExists(TBL)).isFalse();

    // use the bearer token for config
    Mockito.verify(adapter)
        .execute(
            reqMatcher(HTTPMethod.GET, "v1/config", catalogHeaders),
            eq(ConfigResponse.class),
            any(),
            any());
    // use the bearer token to fetch the context token
    Mockito.verify(adapter)
        .execute(
            reqMatcher(HTTPMethod.POST, oauth2ServerUri, catalogHeaders),
            eq(OAuthTokenResponse.class),
            any(),
            any());
    // use the context token for table existence check
    Mockito.verify(adapter)
        .execute(
            reqMatcher(HTTPMethod.HEAD, RESOURCE_PATHS.table(TBL), contextHeaders),
            any(),
            any(),
            any());
  }

  @ParameterizedTest
  @ValueSource(strings = {"v1/oauth/tokens", "https://auth-server.com/token"})
  public void testCatalogCredentialWithClientCredential(String oauth2ServerUri) {
    Map<String, String> emptyHeaders = ImmutableMap.of();
    Map<String, String> contextHeaders =
        ImmutableMap.of("Authorization", "Bearer client-credentials-token:sub=user");
    Map<String, String> catalogHeaders =
        ImmutableMap.of("Authorization", "Bearer client-credentials-token:sub=catalog");

    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));

    SessionCatalog.SessionContext context =
        new SessionCatalog.SessionContext(
            UUID.randomUUID().toString(),
            "user",
            ImmutableMap.of("credential", "user:secret"),
            ImmutableMap.of());

    RESTCatalog catalog = new RESTCatalog(context, (config) -> adapter);
    catalog.initialize(
        "prod",
        ImmutableMap.of(
            CatalogProperties.URI,
            "ignored",
            "credential",
            "catalog:secret",
            OAuth2Properties.OAUTH2_SERVER_URI,
            oauth2ServerUri));

    assertThat(catalog.tableExists(TBL)).isFalse();

    // call client credentials with no initial auth
    Mockito.verify(adapter)
        .execute(
            reqMatcher(HTTPMethod.POST, oauth2ServerUri, emptyHeaders),
            eq(OAuthTokenResponse.class),
            any(),
            any());
    // use the client credential token for config
    Mockito.verify(adapter)
        .execute(
            reqMatcher(HTTPMethod.GET, "v1/config", catalogHeaders),
            eq(ConfigResponse.class),
            any(),
            any());
    // use the client credential to fetch the context token
    Mockito.verify(adapter)
        .execute(
            reqMatcher(HTTPMethod.POST, oauth2ServerUri, catalogHeaders),
            eq(OAuthTokenResponse.class),
            any(),
            any());
    // use the context token for table existence check
    Mockito.verify(adapter)
        .execute(
            reqMatcher(HTTPMethod.HEAD, RESOURCE_PATHS.table(TBL), contextHeaders),
            any(),
            any(),
            any());
  }

  @ParameterizedTest
  @ValueSource(strings = {"v1/oauth/tokens", "https://auth-server.com/token"})
  public void testCatalogBearerTokenAndCredentialWithClientCredential(String oauth2ServerUri) {
    Map<String, String> contextHeaders =
        ImmutableMap.of("Authorization", "Bearer client-credentials-token:sub=user");
    Map<String, String> initHeaders = ImmutableMap.of("Authorization", "Bearer bearer-token");
    Map<String, String> catalogHeaders =
        ImmutableMap.of("Authorization", "Bearer client-credentials-token:sub=catalog");

    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));

    SessionCatalog.SessionContext context =
        new SessionCatalog.SessionContext(
            UUID.randomUUID().toString(),
            "user",
            ImmutableMap.of("credential", "user:secret"),
            ImmutableMap.of());

    RESTCatalog catalog = new RESTCatalog(context, (config) -> adapter);
    catalog.initialize(
        "prod",
        ImmutableMap.of(
            CatalogProperties.URI,
            "ignored",
            "credential",
            "catalog:secret",
            "token",
            "bearer-token",
            OAuth2Properties.OAUTH2_SERVER_URI,
            oauth2ServerUri));

    assertThat(catalog.tableExists(TBL)).isFalse();

    // use the bearer token for client credentials
    Mockito.verify(adapter)
        .execute(
            reqMatcher(HTTPMethod.POST, oauth2ServerUri, initHeaders),
            eq(OAuthTokenResponse.class),
            any(),
            any());
    // use the client credential token for config
    Mockito.verify(adapter)
        .execute(
            reqMatcher(HTTPMethod.GET, "v1/config", catalogHeaders),
            eq(ConfigResponse.class),
            any(),
            any());
    // use the client credential to fetch the context token
    Mockito.verify(adapter)
        .execute(
            reqMatcher(HTTPMethod.POST, oauth2ServerUri, catalogHeaders),
            eq(OAuthTokenResponse.class),
            any(),
            any());
    // use the context token for table existence check
    Mockito.verify(adapter)
        .execute(
            reqMatcher(HTTPMethod.HEAD, RESOURCE_PATHS.table(TBL), contextHeaders),
            any(),
            any(),
            any());
  }

  @ParameterizedTest
  @ValueSource(strings = {"v1/oauth/tokens", "https://auth-server.com/token"})
  public void testClientBearerToken(String oauth2ServerUri) {
    testClientAuth(
        "bearer-token",
        ImmutableMap.of(
            "token", "client-bearer-token",
            "credential", "user:secret",
            "urn:ietf:params:oauth:token-type:id_token", "id-token",
            "urn:ietf:params:oauth:token-type:access_token", "access-token",
            "urn:ietf:params:oauth:token-type:jwt", "jwt-token",
            "urn:ietf:params:oauth:token-type:saml2", "saml2-token",
            "urn:ietf:params:oauth:token-type:saml1", "saml1-token"),
        ImmutableMap.of("Authorization", "Bearer client-bearer-token"),
        oauth2ServerUri,
        ImmutableMap.of());
  }

  @ParameterizedTest
  @ValueSource(strings = {"v1/oauth/tokens", "https://auth-server.com/token"})
  public void testClientCredential(String oauth2ServerUri) {
    testClientAuth(
        "bearer-token",
        ImmutableMap.of(
            "credential", "user:secret",
            "urn:ietf:params:oauth:token-type:id_token", "id-token",
            "urn:ietf:params:oauth:token-type:access_token", "access-token",
            "urn:ietf:params:oauth:token-type:jwt", "jwt-token",
            "urn:ietf:params:oauth:token-type:saml2", "saml2-token",
            "urn:ietf:params:oauth:token-type:saml1", "saml1-token"),
        ImmutableMap.of("Authorization", "Bearer client-credentials-token:sub=user"),
        oauth2ServerUri,
        ImmutableMap.of());
  }

  @ParameterizedTest
  @ValueSource(strings = {"v1/oauth/tokens", "https://auth-server.com/token"})
  public void testClientIDToken(String oauth2ServerUri) {
    testClientAuth(
        "bearer-token",
        ImmutableMap.of(
            "urn:ietf:params:oauth:token-type:id_token", "id-token",
            "urn:ietf:params:oauth:token-type:access_token", "access-token",
            "urn:ietf:params:oauth:token-type:jwt", "jwt-token",
            "urn:ietf:params:oauth:token-type:saml2", "saml2-token",
            "urn:ietf:params:oauth:token-type:saml1", "saml1-token"),
        ImmutableMap.of(
            "Authorization", "Bearer token-exchange-token:sub=id-token,act=bearer-token"),
        oauth2ServerUri,
        ImmutableMap.of());
  }

  @ParameterizedTest
  @ValueSource(strings = {"v1/oauth/tokens", "https://auth-server.com/token"})
  public void testClientAccessToken(String oauth2ServerUri) {
    testClientAuth(
        "bearer-token",
        ImmutableMap.of(
            "urn:ietf:params:oauth:token-type:access_token", "access-token",
            "urn:ietf:params:oauth:token-type:jwt", "jwt-token",
            "urn:ietf:params:oauth:token-type:saml2", "saml2-token",
            "urn:ietf:params:oauth:token-type:saml1", "saml1-token"),
        ImmutableMap.of(
            "Authorization", "Bearer token-exchange-token:sub=access-token,act=bearer-token"),
        oauth2ServerUri,
        ImmutableMap.of());
  }

  @ParameterizedTest
  @ValueSource(strings = {"v1/oauth/tokens", "https://auth-server.com/token"})
  public void testClientAccessTokenWithOptionalParams(String oauth2ServerUri) {
    testClientAuth(
        "bearer-token",
        ImmutableMap.of(
            "urn:ietf:params:oauth:token-type:access_token", "access-token",
            "urn:ietf:params:oauth:token-type:jwt", "jwt-token",
            "urn:ietf:params:oauth:token-type:saml2", "saml2-token",
            "urn:ietf:params:oauth:token-type:saml1", "saml1-token"),
        ImmutableMap.of(
            "Authorization", "Bearer token-exchange-token:sub=access-token,act=bearer-token"),
        oauth2ServerUri,
        ImmutableMap.of(
            "scope", "custom_scope", "audience", "test_audience", "resource", "test_resource"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"v1/oauth/tokens", "https://auth-server.com/token"})
  public void testClientJWTToken(String oauth2ServerUri) {
    testClientAuth(
        "bearer-token",
        ImmutableMap.of(
            "urn:ietf:params:oauth:token-type:jwt", "jwt-token",
            "urn:ietf:params:oauth:token-type:saml2", "saml2-token",
            "urn:ietf:params:oauth:token-type:saml1", "saml1-token"),
        ImmutableMap.of(
            "Authorization", "Bearer token-exchange-token:sub=jwt-token,act=bearer-token"),
        oauth2ServerUri,
        ImmutableMap.of());
  }

  @ParameterizedTest
  @ValueSource(strings = {"v1/oauth/tokens", "https://auth-server.com/token"})
  public void testClientSAML2Token(String oauth2ServerUri) {
    testClientAuth(
        "bearer-token",
        ImmutableMap.of(
            "urn:ietf:params:oauth:token-type:saml2", "saml2-token",
            "urn:ietf:params:oauth:token-type:saml1", "saml1-token"),
        ImmutableMap.of(
            "Authorization", "Bearer token-exchange-token:sub=saml2-token,act=bearer-token"),
        oauth2ServerUri,
        ImmutableMap.of());
  }

  @ParameterizedTest
  @ValueSource(strings = {"v1/oauth/tokens", "https://auth-server.com/token"})
  public void testClientSAML1Token(String oauth2ServerUri) {
    testClientAuth(
        "bearer-token",
        ImmutableMap.of("urn:ietf:params:oauth:token-type:saml1", "saml1-token"),
        ImmutableMap.of(
            "Authorization", "Bearer token-exchange-token:sub=saml1-token,act=bearer-token"),
        oauth2ServerUri,
        ImmutableMap.of());
  }

  private void testClientAuth(
      String catalogToken,
      Map<String, String> credentials,
      Map<String, String> expectedHeaders,
      String oauth2ServerUri,
      Map<String, String> optionalOAuthParams) {
    Map<String, String> catalogHeaders = ImmutableMap.of("Authorization", "Bearer " + catalogToken);

    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));

    SessionCatalog.SessionContext context =
        new SessionCatalog.SessionContext(
            UUID.randomUUID().toString(), "user", credentials, ImmutableMap.of());

    RESTCatalog catalog = new RESTCatalog(context, (config) -> adapter);

    ImmutableMap.Builder<String, String> propertyBuilder = ImmutableMap.builder();
    Map<String, String> initializationProperties =
        propertyBuilder
            .put(CatalogProperties.URI, "ignored")
            .put("token", catalogToken)
            .put(OAuth2Properties.OAUTH2_SERVER_URI, oauth2ServerUri)
            .putAll(optionalOAuthParams)
            .build();
    catalog.initialize("prod", initializationProperties);

    assertThat(catalog.tableExists(TBL)).isFalse();

    Mockito.verify(adapter)
        .execute(
            reqMatcher(HTTPMethod.GET, "v1/config", catalogHeaders),
            eq(ConfigResponse.class),
            any(),
            any());

    // token passes a static token. otherwise, validate a client credentials or token exchange
    // request
    if (!credentials.containsKey("token")) {
      Mockito.verify(adapter)
          .execute(
              reqMatcher(HTTPMethod.POST, oauth2ServerUri, catalogHeaders),
              eq(OAuthTokenResponse.class),
              any(),
              any());
    }
    Mockito.verify(adapter)
        .execute(
            reqMatcher(HTTPMethod.HEAD, RESOURCE_PATHS.table(TBL), expectedHeaders),
            any(),
            any(),
            any());
    if (!optionalOAuthParams.isEmpty()) {
      Mockito.verify(adapter)
          .postForm(
              eq(oauth2ServerUri),
              Mockito.argThat(body -> body.keySet().containsAll(optionalOAuthParams.keySet())),
              eq(OAuthTokenResponse.class),
              eq(catalogHeaders),
              any());
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"v1/oauth/tokens", "https://auth-server.com/token"})
  public void testTableBearerToken(String oauth2ServerUri) {
    testTableAuth(
        "catalog",
        ImmutableMap.of("urn:ietf:params:oauth:token-type:id_token", "id-token"),
        ImmutableMap.of("token", "table-bearer-token"),
        ImmutableMap.of("Authorization", "Bearer token-exchange-token:sub=id-token,act=catalog"),
        ImmutableMap.of("Authorization", "Bearer table-bearer-token"),
        oauth2ServerUri);
  }

  @ParameterizedTest
  @ValueSource(strings = {"v1/oauth/tokens", "https://auth-server.com/token"})
  public void testTableIDToken(String oauth2ServerUri) {
    testTableAuth(
        "catalog",
        ImmutableMap.of("urn:ietf:params:oauth:token-type:id_token", "id-token"),
        ImmutableMap.of("urn:ietf:params:oauth:token-type:id_token", "table-id-token"),
        ImmutableMap.of("Authorization", "Bearer token-exchange-token:sub=id-token,act=catalog"),
        ImmutableMap.of(
            "Authorization",
            "Bearer token-exchange-token:sub=table-id-token,act=token-exchange-token:sub=id-token,act=catalog"),
        oauth2ServerUri);
  }

  @ParameterizedTest
  @ValueSource(strings = {"v1/oauth/tokens", "https://auth-server.com/token"})
  public void testTableCredential(String oauth2ServerUri) {
    testTableAuth(
        "catalog",
        ImmutableMap.of("urn:ietf:params:oauth:token-type:id_token", "id-token"),
        ImmutableMap.of("credential", "table-user:secret"), // will be ignored
        ImmutableMap.of("Authorization", "Bearer token-exchange-token:sub=id-token,act=catalog"),
        ImmutableMap.of("Authorization", "Bearer token-exchange-token:sub=id-token,act=catalog"),
        oauth2ServerUri);
  }

  @Test
  public void testSnapshotParams() {
    assertThat(SnapshotMode.ALL.params()).isEqualTo(ImmutableMap.of("snapshots", "all"));

    assertThat(SnapshotMode.REFS.params()).isEqualTo(ImmutableMap.of("snapshots", "refs"));
  }

  @Test
  public void testTableSnapshotLoading() {
    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));

    RESTCatalog catalog =
        new RESTCatalog(SessionCatalog.SessionContext.createEmpty(), (config) -> adapter);
    catalog.initialize(
        "test",
        ImmutableMap.of(
            CatalogProperties.URI,
            "ignored",
            CatalogProperties.FILE_IO_IMPL,
            "org.apache.iceberg.inmemory.InMemoryFileIO",
            // default loading to refs only
            "snapshot-loading-mode",
            "refs"));

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(TABLE.namespace());
    }

    // Create a table with multiple snapshots
    Table table = catalog.createTable(TABLE, SCHEMA);
    table
        .newFastAppend()
        .appendFile(
            DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath("/path/to/data-a.parquet")
                .withFileSizeInBytes(10)
                .withRecordCount(2)
                .build())
        .commit();

    table
        .newFastAppend()
        .appendFile(
            DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath("/path/to/data-b.parquet")
                .withFileSizeInBytes(10)
                .withRecordCount(2)
                .build())
        .commit();

    ResourcePaths paths = ResourcePaths.forCatalogProperties(Maps.newHashMap());

    // Respond with only referenced snapshots
    Answer<?> refsAnswer =
        invocation -> {
          LoadTableResponse originalResponse = (LoadTableResponse) invocation.callRealMethod();
          TableMetadata refsMetadata =
              TableMetadata.buildFrom(originalResponse.tableMetadata())
                  .suppressHistoricalSnapshots()
                  .build();

          // don't call snapshots() directly as that would cause to load all snapshots. Instead,
          // make sure the snapshots field holds exactly 1 snapshot
          assertThat(refsMetadata)
              .extracting("snapshots")
              .asInstanceOf(InstanceOfAssertFactories.list(Snapshot.class))
              .hasSize(1);

          return LoadTableResponse.builder()
              .withTableMetadata(refsMetadata)
              .addAllConfig(originalResponse.config())
              .build();
        };

    Mockito.doAnswer(refsAnswer)
        .when(adapter)
        .execute(
            reqMatcher(HTTPMethod.GET, paths.table(TABLE), Map.of(), Map.of("snapshots", "refs")),
            eq(LoadTableResponse.class),
            any(),
            any());

    Table refsTables = catalog.loadTable(TABLE);

    assertThat(refsTables.currentSnapshot()).isEqualTo(table.currentSnapshot());
    // verify that the table was loaded with the refs argument
    verify(adapter, times(1))
        .execute(
            reqMatcher(HTTPMethod.GET, paths.table(TABLE), Map.of(), Map.of("snapshots", "refs")),
            eq(LoadTableResponse.class),
            any(),
            any());

    // verify that all snapshots are loaded when referenced
    assertThat(refsTables.snapshots()).containsExactlyInAnyOrderElementsOf(table.snapshots());
    verify(adapter, times(1))
        .execute(
            reqMatcher(HTTPMethod.GET, paths.table(TABLE), Map.of(), Map.of("snapshots", "all")),
            eq(LoadTableResponse.class),
            any(),
            any());
  }

  @ParameterizedTest
  @ValueSource(strings = {"1", "2"})
  public void testTableSnapshotLoadingWithDivergedBranches(String formatVersion) {
    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));

    RESTCatalog catalog =
        new RESTCatalog(SessionCatalog.SessionContext.createEmpty(), (config) -> adapter);
    catalog.initialize(
        "test",
        ImmutableMap.of(
            CatalogProperties.URI,
            "ignored",
            CatalogProperties.FILE_IO_IMPL,
            "org.apache.iceberg.inmemory.InMemoryFileIO",
            "snapshot-loading-mode",
            "refs"));

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(TABLE.namespace());
    }

    Table table =
        catalog.createTable(
            TABLE,
            SCHEMA,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of("format-version", formatVersion));

    table
        .newFastAppend()
        .appendFile(
            DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath("/path/to/data-a.parquet")
                .withFileSizeInBytes(10)
                .withRecordCount(2)
                .build())
        .commit();

    String branch = "divergedBranch";
    table.manageSnapshots().createBranch(branch, table.currentSnapshot().snapshotId()).commit();

    table
        .newFastAppend()
        .appendFile(
            DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath("/path/to/data-b.parquet")
                .withFileSizeInBytes(10)
                .withRecordCount(2)
                .build())
        .toBranch(branch)
        .commit();

    table
        .newFastAppend()
        .appendFile(
            DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath("/path/to/data-c.parquet")
                .withFileSizeInBytes(10)
                .withRecordCount(2)
                .build())
        .toBranch(branch)
        .commit();

    ResourcePaths paths = ResourcePaths.forCatalogProperties(Maps.newHashMap());

    // Respond with only referenced snapshots
    Answer<?> refsAnswer =
        invocation -> {
          LoadTableResponse originalResponse = (LoadTableResponse) invocation.callRealMethod();
          TableMetadata refsMetadata =
              TableMetadata.buildFrom(originalResponse.tableMetadata())
                  .suppressHistoricalSnapshots()
                  .build();

          // don't call snapshots() directly as that would cause to load all snapshots. Instead,
          // make sure the snapshots field holds exactly 2 snapshots (the latest snapshot for main
          // and the branch)
          assertThat(refsMetadata)
              .extracting("snapshots")
              .asInstanceOf(InstanceOfAssertFactories.list(Snapshot.class))
              .hasSize(2);

          return LoadTableResponse.builder()
              .withTableMetadata(refsMetadata)
              .addAllConfig(originalResponse.config())
              .build();
        };

    Mockito.doAnswer(refsAnswer)
        .when(adapter)
        .execute(
            reqMatcher(HTTPMethod.GET, paths.table(TABLE), Map.of(), Map.of("snapshots", "refs")),
            eq(LoadTableResponse.class),
            any(),
            any());

    Table refsTables = catalog.loadTable(TABLE);
    assertThat(refsTables.currentSnapshot()).isEqualTo(table.currentSnapshot());

    // verify that the table was loaded with the refs argument
    verify(adapter, times(1))
        .execute(
            reqMatcher(HTTPMethod.GET, paths.table(TABLE), Map.of(), Map.of("snapshots", "refs")),
            eq(LoadTableResponse.class),
            any(),
            any());

    // verify that all snapshots are loaded when referenced
    assertThat(catalog.loadTable(TABLE).snapshots())
        .containsExactlyInAnyOrderElementsOf(table.snapshots());
    verify(adapter, times(1))
        .execute(
            reqMatcher(HTTPMethod.GET, paths.table(TABLE), Map.of(), Map.of("snapshots", "all")),
            eq(LoadTableResponse.class),
            any(),
            any());

    // verify that committing to branch is possible
    catalog
        .loadTable(TABLE)
        .newAppend()
        .appendFile(
            DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath("/path/to/data-c.parquet")
                .withFileSizeInBytes(10)
                .withRecordCount(2)
                .build())
        .toBranch(branch)
        .commit();

    assertThat(catalog.loadTable(TABLE).snapshots())
        .hasSizeGreaterThan(Lists.newArrayList(table.snapshots()).size());
  }

  @Test
  public void lazySnapshotLoadingWithDivergedHistory() {
    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));

    RESTCatalog catalog =
        new RESTCatalog(SessionCatalog.SessionContext.createEmpty(), (config) -> adapter);
    catalog.initialize(
        "test",
        ImmutableMap.of(
            CatalogProperties.URI,
            "ignored",
            CatalogProperties.FILE_IO_IMPL,
            "org.apache.iceberg.inmemory.InMemoryFileIO",
            "snapshot-loading-mode",
            "refs"));

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(TABLE.namespace());
    }

    Table table =
        catalog.createTable(TABLE, SCHEMA, PartitionSpec.unpartitioned(), ImmutableMap.of());

    int numSnapshots = 5;

    for (int i = 0; i < numSnapshots; i++) {
      table
          .newFastAppend()
          .appendFile(
              DataFiles.builder(PartitionSpec.unpartitioned())
                  .withPath(String.format("/path/to/data-%s.parquet", i))
                  .withFileSizeInBytes(10)
                  .withRecordCount(2)
                  .build())
          .commit();
    }

    ResourcePaths paths = ResourcePaths.forCatalogProperties(Maps.newHashMap());

    // Respond with only referenced snapshots
    Answer<?> refsAnswer =
        invocation -> {
          LoadTableResponse originalResponse = (LoadTableResponse) invocation.callRealMethod();
          TableMetadata refsMetadata =
              TableMetadata.buildFrom(originalResponse.tableMetadata())
                  .suppressHistoricalSnapshots()
                  .build();

          // don't call snapshots() directly as that would cause to load all snapshots. Instead,
          // make sure the snapshots field holds exactly 1 snapshot
          assertThat(refsMetadata)
              .extracting("snapshots")
              .asInstanceOf(InstanceOfAssertFactories.list(Snapshot.class))
              .hasSize(1);

          return LoadTableResponse.builder()
              .withTableMetadata(refsMetadata)
              .addAllConfig(originalResponse.config())
              .build();
        };

    Mockito.doAnswer(refsAnswer)
        .when(adapter)
        .execute(
            reqMatcher(HTTPMethod.GET, paths.table(TABLE), Map.of(), Map.of("snapshots", "refs")),
            eq(LoadTableResponse.class),
            any(),
            any());

    Table refsTables = catalog.loadTable(TABLE);
    assertThat(refsTables.currentSnapshot()).isEqualTo(table.currentSnapshot());
    assertThat(refsTables.snapshots()).hasSize(numSnapshots);
    assertThat(refsTables.history()).hasSize(numSnapshots);
  }

  @SuppressWarnings("MethodLength")
  public void testTableAuth(
      String catalogToken,
      Map<String, String> credentials,
      Map<String, String> tableConfig,
      Map<String, String> expectedContextHeaders,
      Map<String, String> expectedTableHeaders,
      String oauth2ServerUri) {
    Map<String, String> catalogHeaders = ImmutableMap.of("Authorization", "Bearer " + catalogToken);

    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));

    // inject the expected table config
    Answer<LoadTableResponse> addTableConfig =
        invocation -> {
          LoadTableResponse loadTable = (LoadTableResponse) invocation.callRealMethod();
          return LoadTableResponse.builder()
              .withTableMetadata(loadTable.tableMetadata())
              .addAllConfig(loadTable.config())
              .addAllConfig(tableConfig)
              .build();
        };

    Mockito.doAnswer(addTableConfig)
        .when(adapter)
        .execute(
            reqMatcher(HTTPMethod.POST, "v1/namespaces/ns/tables", expectedContextHeaders),
            eq(LoadTableResponse.class),
            any(),
            any());

    Mockito.doAnswer(addTableConfig)
        .when(adapter)
        .execute(
            reqMatcher(HTTPMethod.GET, RESOURCE_PATHS.table(TBL), expectedContextHeaders),
            eq(LoadTableResponse.class),
            any(),
            any());

    SessionCatalog.SessionContext context =
        new SessionCatalog.SessionContext(
            UUID.randomUUID().toString(), "user", credentials, ImmutableMap.of());

    RESTCatalog catalog = new RESTCatalog(context, (config) -> adapter);
    catalog.initialize(
        "prod",
        ImmutableMap.of(
            CatalogProperties.URI,
            "ignored",
            "token",
            catalogToken,
            OAuth2Properties.OAUTH2_SERVER_URI,
            oauth2ServerUri));

    Schema expectedSchema =
        new Schema(
            required(1, "id", Types.IntegerType.get(), "unique ID"),
            required(2, "data", Types.StringType.get()));

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(TBL.namespace());
    }

    Table table = catalog.createTable(TBL, expectedSchema);
    assertThat(table.schema().asStruct())
        .as("Schema should match")
        .isEqualTo(expectedSchema.asStruct());

    Table loaded = catalog.loadTable(TBL); // the first load will send the token
    assertThat(loaded.schema().asStruct())
        .as("Schema should match")
        .isEqualTo(expectedSchema.asStruct());

    loaded.refresh(); // refresh to force reload

    Mockito.verify(adapter)
        .execute(
            reqMatcher(HTTPMethod.GET, "v1/config", catalogHeaders),
            eq(ConfigResponse.class),
            any(),
            any());
    // session client credentials flow
    Mockito.verify(adapter)
        .execute(
            reqMatcher(HTTPMethod.POST, oauth2ServerUri, catalogHeaders),
            eq(OAuthTokenResponse.class),
            any(),
            any());

    // create table request
    Mockito.verify(adapter)
        .execute(
            reqMatcher(HTTPMethod.POST, "v1/namespaces/ns/tables", expectedContextHeaders),
            eq(LoadTableResponse.class),
            any(),
            any());

    // if the table returned a bearer token or a credential, there will be no token request
    if (!tableConfig.containsKey("token") && !tableConfig.containsKey("credential")) {
      // token exchange to get a table token
      Mockito.verify(adapter, times(1))
          .execute(
              reqMatcher(HTTPMethod.POST, oauth2ServerUri, expectedContextHeaders),
              eq(OAuthTokenResponse.class),
              any(),
              any());
    }

    if (expectedContextHeaders.equals(expectedTableHeaders)) {
      // load table from catalog + refresh loaded table
      Mockito.verify(adapter, times(2))
          .execute(
              reqMatcher(HTTPMethod.GET, RESOURCE_PATHS.table(TBL), expectedTableHeaders),
              eq(LoadTableResponse.class),
              any(),
              any());
    } else {
      // load table from catalog
      Mockito.verify(adapter)
          .execute(
              reqMatcher(HTTPMethod.GET, RESOURCE_PATHS.table(TBL), expectedContextHeaders),
              eq(LoadTableResponse.class),
              any(),
              any());

      // refresh loaded table
      Mockito.verify(adapter)
          .execute(
              reqMatcher(HTTPMethod.GET, RESOURCE_PATHS.table(TBL), expectedTableHeaders),
              eq(LoadTableResponse.class),
              any(),
              any());
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"v1/oauth/tokens", "https://auth-server.com/token"})
  public void testCatalogTokenRefresh(String oauth2ServerUri) {
    Map<String, String> emptyHeaders = ImmutableMap.of();
    Map<String, String> catalogHeaders =
        ImmutableMap.of("Authorization", "Bearer client-credentials-token:sub=catalog");

    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));

    Answer<OAuthTokenResponse> addOneSecondExpiration =
        invocation -> {
          OAuthTokenResponse response = (OAuthTokenResponse) invocation.callRealMethod();
          return OAuthTokenResponse.builder()
              .withToken(response.token())
              .withTokenType(response.tokenType())
              .withIssuedTokenType(response.issuedTokenType())
              .addScopes(response.scopes())
              .setExpirationInSeconds(1)
              .build();
        };

    Mockito.doAnswer(addOneSecondExpiration)
        .when(adapter)
        .postForm(eq(oauth2ServerUri), anyMap(), eq(OAuthTokenResponse.class), anyMap(), any());

    Map<String, String> contextCredentials = ImmutableMap.of();
    SessionCatalog.SessionContext context =
        new SessionCatalog.SessionContext(
            UUID.randomUUID().toString(), "user", contextCredentials, ImmutableMap.of());

    RESTCatalog catalog = new RESTCatalog(context, (config) -> adapter);
    catalog.initialize(
        "prod",
        ImmutableMap.of(
            CatalogProperties.URI,
            "ignored",
            "credential",
            "catalog:secret",
            OAuth2Properties.OAUTH2_SERVER_URI,
            oauth2ServerUri));

    Awaitility.await()
        .atMost(5, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              // call client credentials with no initial auth
              Mockito.verify(adapter)
                  .execute(
                      reqMatcher(HTTPMethod.POST, oauth2ServerUri, emptyHeaders),
                      eq(OAuthTokenResponse.class),
                      any(),
                      any());

              // use the client credential token for config
              Mockito.verify(adapter)
                  .execute(
                      reqMatcher(HTTPMethod.GET, "v1/config", catalogHeaders),
                      eq(ConfigResponse.class),
                      any(),
                      any());

              // verify the first token exchange
              Map<String, String> firstRefreshRequest =
                  ImmutableMap.of(
                      "grant_type", "urn:ietf:params:oauth:grant-type:token-exchange",
                      "subject_token", "client-credentials-token:sub=catalog",
                      "subject_token_type", "urn:ietf:params:oauth:token-type:access_token",
                      "scope", "catalog");
              Mockito.verify(adapter)
                  .execute(
                      reqMatcher(
                          HTTPMethod.POST,
                          oauth2ServerUri,
                          catalogHeaders,
                          Map.of(),
                          firstRefreshRequest),
                      eq(OAuthTokenResponse.class),
                      any(),
                      any());

              // verify that a second exchange occurs
              Map<String, String> secondRefreshRequest =
                  ImmutableMap.of(
                      "grant_type", "urn:ietf:params:oauth:grant-type:token-exchange",
                      "subject_token",
                          "token-exchange-token:sub=client-credentials-token:sub=catalog",
                      "subject_token_type", "urn:ietf:params:oauth:token-type:access_token",
                      "scope", "catalog");
              Map<String, String> secondRefreshHeaders =
                  ImmutableMap.of(
                      "Authorization",
                      "Bearer token-exchange-token:sub=client-credentials-token:sub=catalog");
              Mockito.verify(adapter)
                  .execute(
                      reqMatcher(
                          HTTPMethod.POST,
                          oauth2ServerUri,
                          secondRefreshHeaders,
                          Map.of(),
                          secondRefreshRequest),
                      eq(OAuthTokenResponse.class),
                      any(),
                      any());
            });
  }

  @ParameterizedTest
  @ValueSource(strings = {"v1/oauth/tokens", "https://auth-server.com/token"})
  public void testCatalogRefreshedTokenIsUsed(String oauth2ServerUri) {
    Map<String, String> emptyHeaders = ImmutableMap.of();
    Map<String, String> catalogHeaders =
        ImmutableMap.of("Authorization", "Bearer client-credentials-token:sub=catalog");

    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));

    Answer<OAuthTokenResponse> addOneSecondExpiration =
        invocation -> {
          OAuthTokenResponse response = (OAuthTokenResponse) invocation.callRealMethod();
          return OAuthTokenResponse.builder()
              .withToken(response.token())
              .withTokenType(response.tokenType())
              .withIssuedTokenType(response.issuedTokenType())
              .addScopes(response.scopes())
              .setExpirationInSeconds(1)
              .build();
        };

    Mockito.doAnswer(addOneSecondExpiration)
        .when(adapter)
        .postForm(eq(oauth2ServerUri), anyMap(), eq(OAuthTokenResponse.class), anyMap(), any());

    Map<String, String> contextCredentials = ImmutableMap.of();
    SessionCatalog.SessionContext context =
        new SessionCatalog.SessionContext(
            UUID.randomUUID().toString(), "user", contextCredentials, ImmutableMap.of());

    RESTCatalog catalog = new RESTCatalog(context, (config) -> adapter);
    catalog.initialize(
        "prod",
        ImmutableMap.of(
            CatalogProperties.URI,
            "ignored",
            "credential",
            "catalog:secret",
            OAuth2Properties.OAUTH2_SERVER_URI,
            oauth2ServerUri));

    Awaitility.await()
        .atMost(5, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              // use the exchanged catalog token
              assertThat(catalog.tableExists(TBL)).isFalse();

              // call client credentials with no initial auth
              Map<String, String> clientCredentialsRequest =
                  ImmutableMap.of(
                      "grant_type", "client_credentials",
                      "client_id", "catalog",
                      "client_secret", "secret",
                      "scope", "catalog");
              Mockito.verify(adapter)
                  .execute(
                      reqMatcher(
                          HTTPMethod.POST,
                          oauth2ServerUri,
                          emptyHeaders,
                          Map.of(),
                          clientCredentialsRequest),
                      eq(OAuthTokenResponse.class),
                      any(),
                      any());

              // use the client credential token for config
              Mockito.verify(adapter)
                  .execute(
                      reqMatcher(HTTPMethod.GET, "v1/config", catalogHeaders),
                      eq(ConfigResponse.class),
                      any(),
                      any());

              // verify the first token exchange
              Map<String, String> firstRefreshRequest =
                  ImmutableMap.of(
                      "grant_type", "urn:ietf:params:oauth:grant-type:token-exchange",
                      "subject_token", "client-credentials-token:sub=catalog",
                      "subject_token_type", "urn:ietf:params:oauth:token-type:access_token",
                      "scope", "catalog");
              Mockito.verify(adapter)
                  .execute(
                      reqMatcher(
                          HTTPMethod.POST,
                          oauth2ServerUri,
                          catalogHeaders,
                          Map.of(),
                          firstRefreshRequest),
                      eq(OAuthTokenResponse.class),
                      any(),
                      any());

              // use the refreshed context token for table existence check
              Map<String, String> refreshedCatalogHeader =
                  ImmutableMap.of(
                      "Authorization",
                      "Bearer token-exchange-token:sub=client-credentials-token:sub=catalog");
              Mockito.verify(adapter)
                  .execute(
                      reqMatcher(
                          HTTPMethod.HEAD, RESOURCE_PATHS.table(TBL), refreshedCatalogHeader),
                      any(),
                      any(),
                      any());
            });
  }

  @Test
  public void testCatalogExpiredBearerTokenRefreshWithoutCredential() {
    // expires at epoch second = 1
    String token =
        "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyLCJleHAiOjF9.gQADTbdEv-rpDWKSkGLbmafyB5UUjTdm9B_1izpuZ6E";

    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));

    Map<String, String> contextCredentials = ImmutableMap.of("token", token);
    SessionCatalog.SessionContext context =
        new SessionCatalog.SessionContext(
            UUID.randomUUID().toString(), "user", contextCredentials, ImmutableMap.of());

    RESTCatalog catalog = new RESTCatalog(context, (config) -> adapter);

    catalog.initialize("prod", ImmutableMap.of(CatalogProperties.URI, "ignored", "token", token));
  }

  @ParameterizedTest
  @ValueSource(strings = {"v1/oauth/tokens", "https://auth-server.com/token"})
  public void testCatalogExpiredBearerTokenIsRefreshedWithCredential(String oauth2ServerUri) {
    // expires at epoch second = 1
    String token =
        "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyLCJleHAiOjF9.gQADTbdEv-rpDWKSkGLbmafyB5UUjTdm9B_1izpuZ6E";
    Map<String, String> emptyHeaders = ImmutableMap.of();
    Map<String, String> catalogHeaders =
        ImmutableMap.of("Authorization", "Bearer client-credentials-token:sub=catalog");

    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));

    String credential = "catalog:12345";
    Map<String, String> contextCredentials = ImmutableMap.of("token", token);
    SessionCatalog.SessionContext context =
        new SessionCatalog.SessionContext(
            UUID.randomUUID().toString(), "user", contextCredentials, ImmutableMap.of());

    RESTCatalog catalog = new RESTCatalog(context, (config) -> adapter);
    // the init token at the catalog level is a valid token
    catalog.initialize(
        "prod",
        ImmutableMap.of(
            CatalogProperties.URI,
            "ignored",
            "credential",
            credential,
            OAuth2Properties.OAUTH2_SERVER_URI,
            oauth2ServerUri));

    assertThat(catalog.tableExists(TBL)).isFalse();

    // call client credentials with no initial auth
    Map<String, String> clientCredentialsRequest =
        ImmutableMap.of(
            "grant_type", "client_credentials",
            "client_id", "catalog",
            "client_secret", "12345",
            "scope", "catalog");
    Mockito.verify(adapter)
        .execute(
            reqMatcher(
                HTTPMethod.POST, oauth2ServerUri, emptyHeaders, Map.of(), clientCredentialsRequest),
            eq(OAuthTokenResponse.class),
            any(),
            any());

    Mockito.verify(adapter)
        .execute(
            reqMatcher(HTTPMethod.GET, "v1/config", catalogHeaders),
            eq(ConfigResponse.class),
            any(),
            any());

    Map<String, String> firstRefreshRequest =
        ImmutableMap.of(
            "grant_type", "urn:ietf:params:oauth:grant-type:token-exchange",
            "subject_token", token,
            "subject_token_type", "urn:ietf:params:oauth:token-type:access_token",
            "scope", "catalog");
    Mockito.verify(adapter)
        .execute(
            reqMatcher(
                HTTPMethod.POST,
                oauth2ServerUri,
                OAuth2Util.basicAuthHeaders(credential),
                Map.of(),
                firstRefreshRequest),
            eq(OAuthTokenResponse.class),
            any(),
            any());

    // verify that a second exchange occurs
    Map<String, String> secondRefreshRequest =
        ImmutableMap.of(
            "grant_type", "urn:ietf:params:oauth:grant-type:token-exchange",
            "subject_token", token,
            "subject_token_type", "urn:ietf:params:oauth:token-type:access_token",
            "scope", "catalog");
    Mockito.verify(adapter)
        .execute(
            reqMatcher(
                HTTPMethod.POST,
                oauth2ServerUri,
                OAuth2Util.basicAuthHeaders(credential),
                Map.of(),
                secondRefreshRequest),
            eq(OAuthTokenResponse.class),
            any(),
            any());

    Mockito.verify(adapter)
        .execute(
            reqMatcher(
                HTTPMethod.HEAD,
                RESOURCE_PATHS.table(TBL),
                Map.of("Authorization", "Bearer token-exchange-token:sub=" + token)),
            any(),
            any(),
            any());
  }

  @Test
  public void testCatalogValidBearerTokenIsNotRefreshed() {
    // expires at epoch second = 19999999999
    String token =
        "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyLCJleHAiOjE5OTk5OTk5OTk5fQ._3k92KJi2NTyTG6V1s2mzJ__GiQtL36DnzsZSkBdYPw";
    Map<String, String> catalogHeaders = ImmutableMap.of("Authorization", "Bearer " + token);

    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));

    String credential = "catalog:12345";
    Map<String, String> contextCredentials =
        ImmutableMap.of("token", token, "credential", credential);
    SessionCatalog.SessionContext context =
        new SessionCatalog.SessionContext(
            UUID.randomUUID().toString(), "user", contextCredentials, ImmutableMap.of());

    RESTCatalog catalog = new RESTCatalog(context, (config) -> adapter);
    catalog.initialize("prod", ImmutableMap.of(CatalogProperties.URI, "ignored", "token", token));

    assertThat(catalog.tableExists(TBL)).isFalse();

    Mockito.verify(adapter)
        .execute(
            reqMatcher(HTTPMethod.GET, "v1/config", catalogHeaders),
            eq(ConfigResponse.class),
            any(),
            any());

    Mockito.verify(adapter)
        .execute(
            reqMatcher(HTTPMethod.HEAD, RESOURCE_PATHS.table(TBL), OAuth2Util.authHeaders(token)),
            any(),
            any(),
            any());
  }

  @ParameterizedTest
  @ValueSource(strings = {"v1/oauth/tokens", "https://auth-server.com/token"})
  public void testCatalogTokenRefreshFailsAndUsesCredentialForRefresh(String oauth2ServerUri) {
    Map<String, String> emptyHeaders = ImmutableMap.of();
    Map<String, String> catalogHeaders =
        ImmutableMap.of("Authorization", "Bearer client-credentials-token:sub=catalog");

    String credential = "catalog:secret";
    Map<String, String> basicHeaders = OAuth2Util.basicAuthHeaders(credential);

    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));

    Answer<OAuthTokenResponse> addOneSecondExpiration =
        invocation -> {
          OAuthTokenResponse response = (OAuthTokenResponse) invocation.callRealMethod();
          return OAuthTokenResponse.builder()
              .withToken(response.token())
              .withTokenType(response.tokenType())
              .withIssuedTokenType(response.issuedTokenType())
              .addScopes(response.scopes())
              .setExpirationInSeconds(1)
              .build();
        };

    Mockito.doAnswer(addOneSecondExpiration)
        .when(adapter)
        .postForm(eq(oauth2ServerUri), anyMap(), eq(OAuthTokenResponse.class), anyMap(), any());

    Map<String, String> firstRefreshRequest =
        ImmutableMap.of(
            "grant_type", "urn:ietf:params:oauth:grant-type:token-exchange",
            "subject_token", "client-credentials-token:sub=catalog",
            "subject_token_type", "urn:ietf:params:oauth:token-type:access_token",
            "scope", "catalog");

    // simulate that the token expired when it was about to be refreshed
    Mockito.doThrow(new RuntimeException("token expired"))
        .when(adapter)
        .postForm(
            eq(oauth2ServerUri),
            argThat(firstRefreshRequest::equals),
            eq(OAuthTokenResponse.class),
            eq(catalogHeaders),
            any());

    Map<String, String> contextCredentials = ImmutableMap.of();
    SessionCatalog.SessionContext context =
        new SessionCatalog.SessionContext(
            UUID.randomUUID().toString(), "user", contextCredentials, ImmutableMap.of());

    // lower retries
    AuthSessionUtil.setTokenRefreshNumRetries(1);

    RESTCatalog catalog = new RESTCatalog(context, (config) -> adapter);
    catalog.initialize(
        "prod",
        ImmutableMap.of(
            CatalogProperties.URI,
            "ignored",
            "credential",
            credential,
            OAuth2Properties.OAUTH2_SERVER_URI,
            oauth2ServerUri));

    Awaitility.await()
        .atMost(5, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              // use the exchanged catalog token
              assertThat(catalog.tableExists(TBL)).isFalse();

              // call client credentials with no initial auth
              Map<String, String> clientCredentialsRequest =
                  ImmutableMap.of(
                      "grant_type", "client_credentials",
                      "client_id", "catalog",
                      "client_secret", "secret",
                      "scope", "catalog");
              Mockito.verify(adapter)
                  .execute(
                      reqMatcher(
                          HTTPMethod.POST,
                          oauth2ServerUri,
                          emptyHeaders,
                          Map.of(),
                          clientCredentialsRequest),
                      eq(OAuthTokenResponse.class),
                      any(),
                      any());

              // use the client credential token for config
              Mockito.verify(adapter)
                  .execute(
                      reqMatcher(HTTPMethod.GET, "v1/config", catalogHeaders),
                      eq(ConfigResponse.class),
                      any(),
                      any());

              // verify the first token exchange - since an exception is thrown, we're performing
              // retries
              Mockito.verify(adapter, times(2))
                  .postForm(
                      eq(oauth2ServerUri),
                      argThat(firstRefreshRequest::equals),
                      eq(OAuthTokenResponse.class),
                      eq(catalogHeaders),
                      any());

              // here we make sure that the basic auth header is used after token refresh retries
              // failed
              Mockito.verify(adapter)
                  .postForm(
                      eq(oauth2ServerUri),
                      argThat(firstRefreshRequest::equals),
                      eq(OAuthTokenResponse.class),
                      eq(basicHeaders),
                      any());

              // use the refreshed context token for table existence check
              Map<String, String> refreshedCatalogHeader =
                  ImmutableMap.of(
                      "Authorization",
                      "Bearer token-exchange-token:sub=client-credentials-token:sub=catalog");
              Mockito.verify(adapter)
                  .execute(
                      reqMatcher(
                          HTTPMethod.HEAD, RESOURCE_PATHS.table(TBL), refreshedCatalogHeader),
                      any(),
                      any(),
                      any());
            });
  }

  @ParameterizedTest
  @ValueSource(strings = {"v1/oauth/tokens", "https://auth-server.com/token"})
  public void testCatalogWithCustomTokenScope(String oauth2ServerUri) {
    Map<String, String> emptyHeaders = ImmutableMap.of();
    Map<String, String> catalogHeaders =
        ImmutableMap.of("Authorization", "Bearer client-credentials-token:sub=catalog");

    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));

    Answer<OAuthTokenResponse> addOneSecondExpiration =
        invocation -> {
          OAuthTokenResponse response = (OAuthTokenResponse) invocation.callRealMethod();
          return OAuthTokenResponse.builder()
              .withToken(response.token())
              .withTokenType(response.tokenType())
              .withIssuedTokenType(response.issuedTokenType())
              .addScopes(response.scopes())
              .setExpirationInSeconds(1)
              .build();
        };

    Mockito.doAnswer(addOneSecondExpiration)
        .when(adapter)
        .postForm(eq(oauth2ServerUri), anyMap(), eq(OAuthTokenResponse.class), anyMap(), any());

    Map<String, String> contextCredentials = ImmutableMap.of();
    SessionCatalog.SessionContext context =
        new SessionCatalog.SessionContext(
            UUID.randomUUID().toString(), "user", contextCredentials, ImmutableMap.of());

    RESTCatalog catalog = new RESTCatalog(context, (config) -> adapter);
    String scope = "custom_catalog_scope";
    catalog.initialize(
        "prod",
        ImmutableMap.of(
            CatalogProperties.URI,
            "ignored",
            "credential",
            "catalog:secret",
            OAuth2Properties.SCOPE,
            scope,
            OAuth2Properties.OAUTH2_SERVER_URI,
            oauth2ServerUri));

    Awaitility.await()
        .atMost(5, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              // call client credentials with no initial auth
              Mockito.verify(adapter)
                  .postForm(
                      eq(oauth2ServerUri),
                      anyMap(),
                      eq(OAuthTokenResponse.class),
                      eq(emptyHeaders),
                      any());

              // use the client credential token for config
              Mockito.verify(adapter)
                  .execute(
                      reqMatcher(HTTPMethod.GET, "v1/config", catalogHeaders),
                      eq(ConfigResponse.class),
                      any(),
                      any());

              // verify the token exchange uses the right scope
              Map<String, String> firstRefreshRequest =
                  ImmutableMap.of(
                      "grant_type", "urn:ietf:params:oauth:grant-type:token-exchange",
                      "subject_token", "client-credentials-token:sub=catalog",
                      "subject_token_type", "urn:ietf:params:oauth:token-type:access_token",
                      "scope", scope);
              Mockito.verify(adapter)
                  .postForm(
                      eq(oauth2ServerUri),
                      argThat(firstRefreshRequest::equals),
                      eq(OAuthTokenResponse.class),
                      eq(catalogHeaders),
                      any());
            });
  }

  @ParameterizedTest
  @ValueSource(strings = {"v1/oauth/tokens", "https://auth-server.com/token"})
  public void testCatalogTokenRefreshDisabledWithToken(String oauth2ServerUri) {
    String token = "some-token";
    Map<String, String> catalogHeaders = ImmutableMap.of("Authorization", "Bearer " + token);

    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));

    Answer<OAuthTokenResponse> addOneSecondExpiration =
        invocation -> {
          OAuthTokenResponse response = (OAuthTokenResponse) invocation.callRealMethod();
          return OAuthTokenResponse.builder()
              .withToken(response.token())
              .withTokenType(response.tokenType())
              .withIssuedTokenType(response.issuedTokenType())
              .addScopes(response.scopes())
              .setExpirationInSeconds(1)
              .build();
        };

    Mockito.doAnswer(addOneSecondExpiration)
        .when(adapter)
        .postForm(eq(oauth2ServerUri), anyMap(), eq(OAuthTokenResponse.class), anyMap(), any());

    Map<String, String> contextCredentials = ImmutableMap.of();
    SessionCatalog.SessionContext context =
        new SessionCatalog.SessionContext(
            UUID.randomUUID().toString(), "user", contextCredentials, ImmutableMap.of());

    RESTCatalog catalog = new RESTCatalog(context, (config) -> adapter);
    catalog.initialize(
        "prod",
        ImmutableMap.of(
            CatalogProperties.URI,
            "ignored",
            OAuth2Properties.TOKEN,
            token,
            OAuth2Properties.TOKEN_REFRESH_ENABLED,
            "false",
            OAuth2Properties.OAUTH2_SERVER_URI,
            oauth2ServerUri));

    Mockito.verify(adapter)
        .execute(
            reqMatcher(HTTPMethod.GET, "v1/config", catalogHeaders),
            eq(ConfigResponse.class),
            any(),
            any());
  }

  @ParameterizedTest
  @ValueSource(strings = {"v1/oauth/tokens", "https://auth-server.com/token"})
  public void testCatalogTokenRefreshDisabledWithCredential(String oauth2ServerUri) {
    Map<String, String> catalogHeaders =
        ImmutableMap.of("Authorization", "Bearer client-credentials-token:sub=catalog");

    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));

    SessionCatalog.SessionContext context =
        new SessionCatalog.SessionContext(
            UUID.randomUUID().toString(), "user", ImmutableMap.of(), ImmutableMap.of());

    RESTCatalog catalog = new RESTCatalog(context, (config) -> adapter);
    catalog.initialize(
        "prod",
        ImmutableMap.of(
            CatalogProperties.URI,
            "ignored",
            OAuth2Properties.CREDENTIAL,
            "catalog:12345",
            OAuth2Properties.TOKEN_REFRESH_ENABLED,
            "false",
            OAuth2Properties.OAUTH2_SERVER_URI,
            oauth2ServerUri));

    // fetch token from client credential
    Map<String, String> fetchTokenFromCredential =
        ImmutableMap.of(
            "grant_type",
            "client_credentials",
            "client_id",
            "catalog",
            "client_secret",
            "12345",
            "scope",
            "catalog");
    Mockito.verify(adapter)
        .postForm(
            eq(oauth2ServerUri),
            argThat(fetchTokenFromCredential::equals),
            eq(OAuthTokenResponse.class),
            eq(ImmutableMap.of()),
            any());

    Mockito.verify(adapter)
        .execute(
            reqMatcher(HTTPMethod.GET, "v1/config", catalogHeaders),
            eq(ConfigResponse.class),
            any(),
            any());
  }

  @Test
  public void diffAgainstSingleTable() {
    Namespace namespace = Namespace.of("namespace");
    TableIdentifier identifier = TableIdentifier.of(namespace, "multipleDiffsAgainstSingleTable");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(namespace);
    }

    Table table = catalog().buildTable(identifier, SCHEMA).create();
    Transaction transaction = table.newTransaction();

    UpdateSchema updateSchema =
        transaction.updateSchema().addColumn("new_col", Types.LongType.get());
    Schema expectedSchema = updateSchema.apply();
    updateSchema.commit();

    UpdatePartitionSpec updateSpec =
        transaction.updateSpec().addField("shard", Expressions.bucket("id", 16));
    PartitionSpec expectedSpec = updateSpec.apply();
    updateSpec.commit();

    TableCommit tableCommit =
        TableCommit.create(
            identifier,
            ((BaseTransaction) transaction).startMetadata(),
            ((BaseTransaction) transaction).currentMetadata());

    restCatalog.commitTransaction(tableCommit);

    Table loaded = catalog().loadTable(identifier);
    assertThat(loaded.schema().asStruct()).isEqualTo(expectedSchema.asStruct());
    assertThat(loaded.spec().fields()).isEqualTo(expectedSpec.fields());
  }

  @Test
  public void multipleDiffsAgainstMultipleTables() {
    Namespace namespace = Namespace.of("multiDiffNamespace");
    TableIdentifier identifier1 = TableIdentifier.of(namespace, "multiDiffTable1");
    TableIdentifier identifier2 = TableIdentifier.of(namespace, "multiDiffTable2");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(namespace);
    }

    Table table1 = catalog().buildTable(identifier1, SCHEMA).create();
    Table table2 = catalog().buildTable(identifier2, SCHEMA).create();
    Transaction t1Transaction = table1.newTransaction();
    Transaction t2Transaction = table2.newTransaction();

    UpdateSchema updateSchema =
        t1Transaction.updateSchema().addColumn("new_col", Types.LongType.get());
    Schema expectedSchema = updateSchema.apply();
    updateSchema.commit();

    UpdateSchema updateSchema2 =
        t2Transaction.updateSchema().addColumn("new_col2", Types.LongType.get());
    Schema expectedSchema2 = updateSchema2.apply();
    updateSchema2.commit();

    TableCommit tableCommit1 =
        TableCommit.create(
            identifier1,
            ((BaseTransaction) t1Transaction).startMetadata(),
            ((BaseTransaction) t1Transaction).currentMetadata());

    TableCommit tableCommit2 =
        TableCommit.create(
            identifier2,
            ((BaseTransaction) t2Transaction).startMetadata(),
            ((BaseTransaction) t2Transaction).currentMetadata());

    restCatalog.commitTransaction(tableCommit1, tableCommit2);

    assertThat(catalog().loadTable(identifier1).schema().asStruct())
        .isEqualTo(expectedSchema.asStruct());

    assertThat(catalog().loadTable(identifier2).schema().asStruct())
        .isEqualTo(expectedSchema2.asStruct());
  }

  @Test
  public void multipleDiffsAgainstMultipleTablesLastFails() {
    Namespace namespace = Namespace.of("multiDiffNamespace");
    TableIdentifier identifier1 = TableIdentifier.of(namespace, "multiDiffTable1");
    TableIdentifier identifier2 = TableIdentifier.of(namespace, "multiDiffTable2");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(namespace);
    }

    catalog().createTable(identifier1, SCHEMA);
    catalog().createTable(identifier2, SCHEMA);

    Table table1 = catalog().loadTable(identifier1);
    Table table2 = catalog().loadTable(identifier2);
    Schema originalSchemaOne = table1.schema();

    Transaction t1Transaction = catalog().loadTable(identifier1).newTransaction();
    t1Transaction.updateSchema().addColumn("new_col1", Types.LongType.get()).commit();

    Transaction t2Transaction = catalog().loadTable(identifier2).newTransaction();
    t2Transaction.updateSchema().renameColumn("data", "new-column").commit();

    // delete the colum that is being renamed in the above TX to cause a conflict
    table2.updateSchema().deleteColumn("data").commit();
    Schema updatedSchemaTwo = table2.schema();

    TableCommit tableCommit1 =
        TableCommit.create(
            identifier1,
            ((BaseTransaction) t1Transaction).startMetadata(),
            ((BaseTransaction) t1Transaction).currentMetadata());

    TableCommit tableCommit2 =
        TableCommit.create(
            identifier2,
            ((BaseTransaction) t2Transaction).startMetadata(),
            ((BaseTransaction) t2Transaction).currentMetadata());

    assertThatThrownBy(() -> restCatalog.commitTransaction(tableCommit1, tableCommit2))
        .isInstanceOf(CommitFailedException.class)
        .hasMessageContaining("Requirement failed: current schema changed: expected id 0 != 1");

    Schema schema1 = catalog().loadTable(identifier1).schema();
    assertThat(schema1.asStruct()).isEqualTo(originalSchemaOne.asStruct());

    Schema schema2 = catalog().loadTable(identifier2).schema();
    assertThat(schema2.asStruct()).isEqualTo(updatedSchemaTwo.asStruct());
    assertThat(schema2.findField("data")).isNull();
    assertThat(schema2.findField("new-column")).isNull();
    assertThat(schema2.columns()).hasSize(1);
  }

  @Test
  public void testInvalidPageSize() {
    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));
    RESTCatalog catalog =
        new RESTCatalog(SessionCatalog.SessionContext.createEmpty(), (config) -> adapter);
    assertThatThrownBy(
            () ->
                catalog.initialize(
                    "test", ImmutableMap.of(RESTSessionCatalog.REST_PAGE_SIZE, "-1")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            String.format(
                "Invalid value for %s, must be a positive integer",
                RESTSessionCatalog.REST_PAGE_SIZE));
  }

  @ParameterizedTest
  @ValueSource(ints = {21, 30})
  public void testPaginationForListNamespaces(int numberOfItems) {
    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));
    RESTCatalog catalog =
        new RESTCatalog(SessionCatalog.SessionContext.createEmpty(), (config) -> adapter);
    catalog.initialize("test", ImmutableMap.of(RESTSessionCatalog.REST_PAGE_SIZE, "10"));
    String namespaceName = "newdb";

    // create several namespaces for listing and verify
    for (int i = 0; i < numberOfItems; i++) {
      String nameSpaceName = namespaceName + i;
      catalog.createNamespace(Namespace.of(nameSpaceName));
    }

    assertThat(catalog.listNamespaces()).hasSize(numberOfItems);

    Mockito.verify(adapter)
        .execute(
            reqMatcher(HTTPMethod.GET, "v1/config", Map.of(), Map.of()),
            eq(ConfigResponse.class),
            any(),
            any());

    Mockito.verify(adapter, times(numberOfItems))
        .execute(
            reqMatcher(HTTPMethod.POST, "v1/namespaces", Map.of(), Map.of()),
            eq(CreateNamespaceResponse.class),
            any(),
            any());

    // verify initial request with empty pageToken
    Mockito.verify(adapter)
        .handleRequest(
            eq(RESTCatalogAdapter.Route.LIST_NAMESPACES),
            eq(ImmutableMap.of("pageToken", "", "pageSize", "10")),
            any(),
            eq(ListNamespacesResponse.class));

    // verify second request with updated pageToken
    Mockito.verify(adapter)
        .handleRequest(
            eq(RESTCatalogAdapter.Route.LIST_NAMESPACES),
            eq(ImmutableMap.of("pageToken", "10", "pageSize", "10")),
            any(),
            eq(ListNamespacesResponse.class));

    // verify third request with update pageToken
    Mockito.verify(adapter)
        .handleRequest(
            eq(RESTCatalogAdapter.Route.LIST_NAMESPACES),
            eq(ImmutableMap.of("pageToken", "20", "pageSize", "10")),
            any(),
            eq(ListNamespacesResponse.class));
  }

  @ParameterizedTest
  @ValueSource(ints = {21, 30})
  public void testPaginationForListTables(int numberOfItems) {
    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));
    RESTCatalog catalog =
        new RESTCatalog(SessionCatalog.SessionContext.createEmpty(), (config) -> adapter);
    catalog.initialize("test", ImmutableMap.of(RESTSessionCatalog.REST_PAGE_SIZE, "10"));
    String namespaceName = "newdb";
    String tableName = "newtable";
    catalog.createNamespace(Namespace.of(namespaceName));

    // create several tables under namespace for listing and verify
    for (int i = 0; i < numberOfItems; i++) {
      TableIdentifier tableIdentifier = TableIdentifier.of(namespaceName, tableName + i);
      catalog.createTable(tableIdentifier, SCHEMA);
    }

    assertThat(catalog.listTables(Namespace.of(namespaceName))).hasSize(numberOfItems);

    Mockito.verify(adapter)
        .execute(
            reqMatcher(HTTPMethod.GET, "v1/config", Map.of(), Map.of()),
            eq(ConfigResponse.class),
            any(),
            any());

    Mockito.verify(adapter, times(numberOfItems))
        .execute(
            reqMatcher(
                HTTPMethod.POST,
                String.format("v1/namespaces/%s/tables", namespaceName),
                Map.of(),
                Map.of()),
            eq(LoadTableResponse.class),
            any(),
            any());

    // verify initial request with empty pageToken
    Mockito.verify(adapter)
        .handleRequest(
            eq(RESTCatalogAdapter.Route.LIST_TABLES),
            eq(ImmutableMap.of("pageToken", "", "pageSize", "10", "namespace", namespaceName)),
            any(),
            eq(ListTablesResponse.class));

    // verify second request with updated pageToken
    Mockito.verify(adapter)
        .handleRequest(
            eq(RESTCatalogAdapter.Route.LIST_TABLES),
            eq(ImmutableMap.of("pageToken", "10", "pageSize", "10", "namespace", namespaceName)),
            any(),
            eq(ListTablesResponse.class));

    // verify third request with update pageToken
    Mockito.verify(adapter)
        .handleRequest(
            eq(RESTCatalogAdapter.Route.LIST_TABLES),
            eq(ImmutableMap.of("pageToken", "20", "pageSize", "10", "namespace", namespaceName)),
            any(),
            eq(ListTablesResponse.class));
  }

  @Test
  public void testCleanupUncommitedFilesForCleanableFailures() {
    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));
    RESTCatalog catalog = catalog(adapter);

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(TABLE.namespace());
    }

    catalog.createTable(TABLE, SCHEMA);
    DataFile file =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/data-a.parquet")
            .withFileSizeInBytes(10)
            .withRecordCount(2)
            .build();

    Table table = catalog.loadTable(TABLE);
    Mockito.doThrow(new NotAuthorizedException("not authorized"))
        .when(adapter)
        .execute(reqMatcher(HTTPMethod.POST), any(), any(), any());
    assertThatThrownBy(() -> catalog.loadTable(TABLE).newFastAppend().appendFile(file).commit())
        .isInstanceOf(NotAuthorizedException.class)
        .hasMessage("not authorized");

    // Extract the UpdateTableRequest to determine the path of the manifest list that should be
    // cleaned up
    assertThat(allRequests(adapter))
        .anySatisfy(
            req -> {
              assertThat(req.method()).isEqualTo(HTTPMethod.POST);
              assertThat(req.path()).isEqualTo(RESOURCE_PATHS.table(TABLE));
              assertThat(req.body()).isInstanceOf(UpdateTableRequest.class);
              UpdateTableRequest body = (UpdateTableRequest) req.body();
              MetadataUpdate.AddSnapshot addSnapshot =
                  (MetadataUpdate.AddSnapshot) body.updates().get(0);
              assertThatThrownBy(
                      () -> table.io().newInputFile(addSnapshot.snapshot().manifestListLocation()))
                  .isInstanceOf(NotFoundException.class)
                  .hasMessageContaining("No in-memory file found");
            });
  }

  @Test
  public void testNoCleanupForNonCleanableExceptions() {
    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));
    RESTCatalog catalog = catalog(adapter);

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(TABLE.namespace());
    }

    catalog.createTable(TABLE, SCHEMA);
    Table table = catalog.loadTable(TABLE);

    Mockito.doThrow(new ServiceFailureException("some service failure"))
        .when(adapter)
        .execute(reqMatcher(HTTPMethod.POST), any(), any(), any());
    assertThatThrownBy(() -> catalog.loadTable(TABLE).newFastAppend().appendFile(FILE_A).commit())
        .isInstanceOf(ServiceFailureException.class)
        .hasMessage("some service failure");

    // Extract the UpdateTableRequest to determine the path of the manifest list that should still
    // exist even though the commit failed
    assertThat(allRequests(adapter))
        .anySatisfy(
            req -> {
              assertThat(req.method()).isEqualTo(HTTPMethod.POST);
              assertThat(req.path()).isEqualTo(RESOURCE_PATHS.table(TABLE));
              assertThat(req.body()).isInstanceOf(UpdateTableRequest.class);
              UpdateTableRequest body = (UpdateTableRequest) req.body();
              MetadataUpdate.AddSnapshot addSnapshot =
                  (MetadataUpdate.AddSnapshot) body.updates().get(0);
              String manifestListLocation = addSnapshot.snapshot().manifestListLocation();
              assertThat(table.io().newInputFile(manifestListLocation).exists()).isTrue();
            });
  }

  @Test
  public void testCleanupCleanableExceptionsCreate() {
    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));
    RESTCatalog catalog = catalog(adapter);

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(TABLE.namespace());
    }

    catalog.createTable(TABLE, SCHEMA);
    TableIdentifier newTable = TableIdentifier.of(TABLE.namespace(), "some_table");
    Mockito.doThrow(new NotAuthorizedException("not authorized"))
        .when(adapter)
        .execute(reqMatcher(HTTPMethod.POST, RESOURCE_PATHS.table(newTable)), any(), any(), any());

    Transaction createTableTransaction = catalog.newCreateTableTransaction(newTable, SCHEMA);
    createTableTransaction.newAppend().appendFile(FILE_A).commit();
    assertThatThrownBy(createTableTransaction::commitTransaction)
        .isInstanceOf(NotAuthorizedException.class)
        .hasMessage("not authorized");

    assertThat(allRequests(adapter))
        .anySatisfy(
            req -> {
              assertThat(req.method()).isEqualTo(HTTPMethod.POST);
              assertThat(req.path()).isEqualTo(RESOURCE_PATHS.table(newTable));
              assertThat(req.body()).isInstanceOf(UpdateTableRequest.class);
              UpdateTableRequest body = (UpdateTableRequest) req.body();
              Optional<MetadataUpdate> appendSnapshot =
                  body.updates().stream()
                      .filter(update -> update instanceof MetadataUpdate.AddSnapshot)
                      .findFirst();

              assertThat(appendSnapshot).isPresent();
              MetadataUpdate.AddSnapshot addSnapshot =
                  (MetadataUpdate.AddSnapshot) appendSnapshot.get();
              assertThatThrownBy(
                      () ->
                          catalog
                              .loadTable(TABLE)
                              .io()
                              .newInputFile(addSnapshot.snapshot().manifestListLocation()))
                  .isInstanceOf(NotFoundException.class)
                  .hasMessageContaining("No in-memory file found");
            });
  }

  @Test
  public void testNoCleanupForNonCleanableCreateTransaction() {
    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));
    RESTCatalog catalog = catalog(adapter);

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(TABLE.namespace());
    }

    catalog.createTable(TABLE, SCHEMA);
    TableIdentifier newTable = TableIdentifier.of(TABLE.namespace(), "some_table");
    Mockito.doThrow(new ServiceFailureException("some service failure"))
        .when(adapter)
        .execute(reqMatcher(HTTPMethod.POST, RESOURCE_PATHS.table(newTable)), any(), any(), any());

    Transaction createTableTransaction = catalog.newCreateTableTransaction(newTable, SCHEMA);
    createTableTransaction.newAppend().appendFile(FILE_A).commit();
    assertThatThrownBy(createTableTransaction::commitTransaction)
        .isInstanceOf(ServiceFailureException.class)
        .hasMessage("some service failure");

    assertThat(allRequests(adapter))
        .anySatisfy(
            req -> {
              assertThat(req.method()).isEqualTo(HTTPMethod.POST);
              assertThat(req.path()).isEqualTo(RESOURCE_PATHS.table(newTable));
              assertThat(req.body()).isInstanceOf(UpdateTableRequest.class);
              UpdateTableRequest body = (UpdateTableRequest) req.body();
              Optional<MetadataUpdate> appendSnapshot =
                  body.updates().stream()
                      .filter(update -> update instanceof MetadataUpdate.AddSnapshot)
                      .findFirst();
              assertThat(appendSnapshot).isPresent();

              MetadataUpdate.AddSnapshot addSnapshot =
                  (MetadataUpdate.AddSnapshot) appendSnapshot.get();
              String manifestListLocation = addSnapshot.snapshot().manifestListLocation();
              assertThat(catalog.loadTable(TABLE).io().newInputFile(manifestListLocation).exists())
                  .isTrue();
            });
  }

  @Test
  public void testCleanupCleanableExceptionsReplace() {
    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));
    RESTCatalog catalog = catalog(adapter);

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(TABLE.namespace());
    }

    catalog.createTable(TABLE, SCHEMA);
    Mockito.doThrow(new NotAuthorizedException("not authorized"))
        .when(adapter)
        .execute(reqMatcher(HTTPMethod.POST, RESOURCE_PATHS.table(TABLE)), any(), any(), any());

    Transaction replaceTableTransaction = catalog.newReplaceTableTransaction(TABLE, SCHEMA, false);
    replaceTableTransaction.newAppend().appendFile(FILE_A).commit();
    assertThatThrownBy(replaceTableTransaction::commitTransaction)
        .isInstanceOf(NotAuthorizedException.class)
        .hasMessage("not authorized");

    assertThat(allRequests(adapter))
        .anySatisfy(
            req -> {
              assertThat(req.method()).isEqualTo(HTTPMethod.POST);
              assertThat(req.path()).isEqualTo(RESOURCE_PATHS.table(TABLE));
              assertThat(req.body()).isInstanceOf(UpdateTableRequest.class);
              UpdateTableRequest request = (UpdateTableRequest) req.body();
              Optional<MetadataUpdate> appendSnapshot =
                  request.updates().stream()
                      .filter(update -> update instanceof MetadataUpdate.AddSnapshot)
                      .findFirst();

              assertThat(appendSnapshot).isPresent();
              MetadataUpdate.AddSnapshot addSnapshot =
                  (MetadataUpdate.AddSnapshot) appendSnapshot.get();
              String manifestListLocation = addSnapshot.snapshot().manifestListLocation();
              assertThatThrownBy(
                      () -> catalog.loadTable(TABLE).io().newInputFile(manifestListLocation))
                  .isInstanceOf(NotFoundException.class)
                  .hasMessageContaining("No in-memory file found");
            });
  }

  @Test
  public void testNoCleanupForNonCleanableReplaceTransaction() {
    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));
    RESTCatalog catalog = catalog(adapter);

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(TABLE.namespace());
    }

    catalog.createTable(TABLE, SCHEMA);
    Mockito.doThrow(new ServiceFailureException("some service failure"))
        .when(adapter)
        .execute(reqMatcher(HTTPMethod.POST, RESOURCE_PATHS.table(TABLE)), any(), any(), any());

    Transaction replaceTableTransaction = catalog.newReplaceTableTransaction(TABLE, SCHEMA, false);
    replaceTableTransaction.newAppend().appendFile(FILE_A).commit();
    assertThatThrownBy(replaceTableTransaction::commitTransaction)
        .isInstanceOf(ServiceFailureException.class)
        .hasMessage("some service failure");

    assertThat(allRequests(adapter))
        .anySatisfy(
            req -> {
              assertThat(req.method()).isEqualTo(HTTPMethod.POST);
              assertThat(req.path()).isEqualTo(RESOURCE_PATHS.table(TABLE));
              assertThat(req.body()).isInstanceOf(UpdateTableRequest.class);
              UpdateTableRequest request = (UpdateTableRequest) req.body();
              Optional<MetadataUpdate> appendSnapshot =
                  request.updates().stream()
                      .filter(update -> update instanceof MetadataUpdate.AddSnapshot)
                      .findFirst();
              assertThat(appendSnapshot).isPresent();

              MetadataUpdate.AddSnapshot addSnapshot =
                  (MetadataUpdate.AddSnapshot) appendSnapshot.get();
              String manifestListLocation = addSnapshot.snapshot().manifestListLocation();
              assertThat(catalog.loadTable(TABLE).io().newInputFile(manifestListLocation).exists())
                  .isTrue();
            });
  }

  @Test
  public void testNamespaceExistsViaHEADRequest() {
    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));
    RESTCatalog catalog =
        new RESTCatalog(SessionCatalog.SessionContext.createEmpty(), (config) -> adapter);
    catalog.initialize("test", ImmutableMap.of());

    assertThat(catalog.namespaceExists(Namespace.of("non-existing"))).isFalse();

    Mockito.verify(adapter)
        .execute(
            reqMatcher(HTTPMethod.GET, "v1/config", Map.of(), Map.of()),
            eq(ConfigResponse.class),
            any(),
            any());
    Mockito.verify(adapter)
        .execute(
            reqMatcher(HTTPMethod.HEAD, "v1/namespaces/non-existing", Map.of(), Map.of()),
            any(),
            any(),
            any());
  }

  @Test
  public void testNamespaceExistsFallbackToGETRequest() {
    // server indicates support of loading a namespace only via GET, which is
    // what older REST servers would send back too
    verifyNamespaceExistsFallbackToGETRequest(
        ConfigResponse.builder()
            .withEndpoints(ImmutableList.of(Endpoint.V1_LOAD_NAMESPACE))
            .build());
  }

  private void verifyNamespaceExistsFallbackToGETRequest(ConfigResponse configResponse) {
    RESTCatalogAdapter adapter =
        Mockito.spy(
            new RESTCatalogAdapter(backendCatalog) {
              @Override
              public <T extends RESTResponse> T execute(
                  HTTPRequest request,
                  Class<T> responseType,
                  Consumer<ErrorResponse> errorHandler,
                  Consumer<Map<String, String>> responseHeaders) {
                if ("v1/config".equals(request.path())) {
                  return castResponse(responseType, configResponse);
                }

                return super.execute(request, responseType, errorHandler, responseHeaders);
              }
            });

    RESTCatalog catalog =
        new RESTCatalog(SessionCatalog.SessionContext.createEmpty(), (config) -> adapter);
    catalog.initialize("test", ImmutableMap.of());

    assertThat(catalog.namespaceExists(Namespace.of("non-existing"))).isFalse();

    Mockito.verify(adapter)
        .execute(
            reqMatcher(HTTPMethod.GET, "v1/config", Map.of(), Map.of()),
            eq(ConfigResponse.class),
            any(),
            any());

    // verifies that the namespace is loaded via a GET instead of HEAD (V1_NAMESPACE_EXISTS)
    Mockito.verify(adapter)
        .execute(
            reqMatcher(HTTPMethod.GET, "v1/namespaces/non-existing", Map.of(), Map.of()),
            any(),
            any(),
            any());
  }

  @Test
  public void testNamespaceExistsFallbackToGETRequestWithLegacyServer() {
    // simulate a legacy server that doesn't send back supported endpoints, thus the
    // client relies on the default endpoints
    verifyNamespaceExistsFallbackToGETRequest(ConfigResponse.builder().build());
  }

  @Test
  public void testTableExistsViaHEADRequest() {
    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));
    RESTCatalog catalog =
        new RESTCatalog(SessionCatalog.SessionContext.createEmpty(), (config) -> adapter);
    catalog.initialize("test", ImmutableMap.of());

    assertThat(catalog.tableExists(TABLE)).isFalse();

    Mockito.verify(adapter)
        .execute(
            reqMatcher(HTTPMethod.GET, "v1/config", Map.of(), Map.of()),
            eq(ConfigResponse.class),
            any(),
            any());
    Mockito.verify(adapter)
        .execute(
            reqMatcher(HTTPMethod.HEAD, RESOURCE_PATHS.table(TABLE), Map.of(), Map.of()),
            any(),
            any(),
            any());
  }

  @Test
  public void testTableExistsFallbackToGETRequest() {
    // server indicates support of loading a table only via GET, which is
    // what older REST servers would send back too
    verifyTableExistsFallbackToGETRequest(
        ConfigResponse.builder().withEndpoints(ImmutableList.of(Endpoint.V1_LOAD_TABLE)).build());
  }

  private void verifyTableExistsFallbackToGETRequest(ConfigResponse configResponse) {
    RESTCatalogAdapter adapter =
        Mockito.spy(
            new RESTCatalogAdapter(backendCatalog) {
              @Override
              public <T extends RESTResponse> T execute(
                  HTTPRequest request,
                  Class<T> responseType,
                  Consumer<ErrorResponse> errorHandler,
                  Consumer<Map<String, String>> responseHeaders) {
                if ("v1/config".equals(request.path())) {
                  return castResponse(responseType, configResponse);
                }

                return super.execute(request, responseType, errorHandler, responseHeaders);
              }
            });

    RESTCatalog catalog =
        new RESTCatalog(SessionCatalog.SessionContext.createEmpty(), (config) -> adapter);
    catalog.initialize("test", ImmutableMap.of());

    assertThat(catalog.tableExists(TABLE)).isFalse();

    Mockito.verify(adapter)
        .execute(
            reqMatcher(HTTPMethod.GET, "v1/config", Map.of(), Map.of()),
            eq(ConfigResponse.class),
            any(),
            any());

    // verifies that the table is loaded via a GET instead of HEAD (V1_LOAD_TABLE)
    Mockito.verify(adapter)
        .execute(
            reqMatcher(
                HTTPMethod.GET, RESOURCE_PATHS.table(TABLE), Map.of(), Map.of("snapshots", "all")),
            any(),
            any(),
            any());
  }

  @Test
  public void testTableExistsFallbackToGETRequestWithLegacyServer() {
    // simulate a legacy server that doesn't send back supported endpoints, thus the
    // client relies on the default endpoints
    verifyTableExistsFallbackToGETRequest(ConfigResponse.builder().build());
  }

  private RESTCatalog catalog(RESTCatalogAdapter adapter) {
    RESTCatalog catalog =
        new RESTCatalog(SessionCatalog.SessionContext.createEmpty(), (config) -> adapter);
    catalog.initialize(
        "test",
        ImmutableMap.of(
            CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.inmemory.InMemoryFileIO"));
    return catalog;
  }

  static HTTPRequest reqMatcher(HTTPMethod method) {
    return argThat(req -> req.method() == method);
  }

  static HTTPRequest reqMatcher(HTTPMethod method, String path) {
    return argThat(req -> req.method() == method && req.path().equals(path));
  }

  static HTTPRequest reqMatcher(HTTPMethod method, String path, Map<String, String> headers) {
    return argThat(
        req ->
            req.method() == method
                && req.path().equals(path)
                && req.headers().equals(HTTPHeaders.of(headers)));
  }

  static HTTPRequest reqMatcher(
      HTTPMethod method, String path, Map<String, String> headers, Map<String, String> parameters) {
    return argThat(
        req ->
            req.method() == method
                && req.path().equals(path)
                && req.headers().equals(HTTPHeaders.of(headers))
                && req.queryParameters().equals(parameters));
  }

  static HTTPRequest reqMatcher(
      HTTPMethod method,
      String path,
      Map<String, String> headers,
      Map<String, String> parameters,
      Object body) {
    return argThat(
        req ->
            req.method() == method
                && req.path().equals(path)
                && req.headers().equals(HTTPHeaders.of(headers))
                && req.queryParameters().equals(parameters)
                && Objects.equals(req.body(), body));
  }

  private static List<HTTPRequest> allRequests(RESTCatalogAdapter adapter) {
    ArgumentCaptor<HTTPRequest> captor = ArgumentCaptor.forClass(HTTPRequest.class);
    verify(adapter, atLeastOnce()).execute(captor.capture(), any(), any(), any());
    return captor.getAllValues();
  }
}
