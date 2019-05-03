/*
 * Copyright 2010 Proofpoint, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.proofpoint.discovery;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.inject.Injector;
import com.proofpoint.bootstrap.LifeCycleManager;
import com.proofpoint.http.client.HttpClient;
import com.proofpoint.http.client.jetty.JettyHttpClient;
import com.proofpoint.http.server.testing.TestingHttpServer;
import com.proofpoint.http.server.testing.TestingHttpServerModule;
import com.proofpoint.jaxrs.JaxrsModule;
import com.proofpoint.json.JsonCodec;
import com.proofpoint.json.JsonModule;
import com.proofpoint.node.NodeInfo;
import com.proofpoint.node.testing.TestingNodeModule;
import com.proofpoint.reporting.ReportingModule;
import com.proofpoint.testing.Closeables;
import org.mockito.Mock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.weakref.jmx.testing.TestingMBeanModule;

import java.net.URI;
import java.util.Map;
import java.util.stream.Stream;

import static com.proofpoint.bootstrap.Bootstrap.bootstrapTest;
import static com.proofpoint.http.client.JsonResponseHandler.createJsonResponseHandler;
import static com.proofpoint.http.client.Request.Builder.prepareGet;
import static com.proofpoint.json.JsonCodec.mapJsonCodec;
import static javax.ws.rs.core.Response.Status.OK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

@SuppressWarnings("unchecked")
public class TestDynamicStoreDisabled
{
    private final HttpClient client = new JettyHttpClient();
    private final JsonCodec<Map<String, Object>> mapCodec = mapJsonCodec(String.class, Object.class);

    private LifeCycleManager lifeCycleManager;
    private TestingHttpServer server;
    private String environment;

    @Mock
    private ConfigStore configStore;
    @Mock
    private ProxyStore proxyStore;


    @BeforeMethod
    public void setup()
            throws Exception
    {
        initMocks(this);

        Injector injector = bootstrapTest()
                .withModules(
                        new TestingNodeModule(),
                        new TestingHttpServerModule(),
                        new JsonModule(),
                        new JaxrsModule(),
                        new ReportingModule(),
                        new TestingMBeanModule(),
                        new DiscoveryServerModule()
                )
                .setRequiredConfigurationProperty("discovery.dynamic.enabled", "false")
                .initialize();

        lifeCycleManager = injector.getInstance(LifeCycleManager.class);
        server = injector.getInstance(TestingHttpServer.class);
        environment = injector.getInstance(NodeInfo.class).getEnvironment();
    }

    @AfterMethod
    public void teardown()
            throws Exception
    {
        if (lifeCycleManager != null) {
            lifeCycleManager.stop();
        }
    }

    @AfterClass(alwaysRun = true)
    public void teardownClass()
    {
        Closeables.closeQuietly(client);
    }

    @Test
    public void testGetByType()
    {
        Map<String, Object> actual = client.execute(
                prepareGet().setUri(uriFor("/v1/service/storage")).build(),
                createJsonResponseHandler(mapCodec, OK.getStatusCode()));
        assertThat(actual.keySet()).containsExactly("environment", "services");
        assertThat(actual.get("environment")).isEqualTo(environment);
        assertThat((Iterable<Object>) actual.get("services")).isEmpty();
    }

    @Test
    public void testGetByTypeAndPool()
    {
        Map<String, Object> actual = client.execute(
                prepareGet().setUri(uriFor("/v1/service/storage/alpha")).build(),
                createJsonResponseHandler(mapCodec, OK.getStatusCode()));
        assertThat(actual.keySet()).containsExactly("environment", "services");
        assertThat(actual.get("environment")).isEqualTo(environment);
        assertThat((Iterable<Object>) actual.get("services")).isEmpty();
    }

    @Test
    public void testGetByTypeAndGeneralPool()
    {
    }

    @Test
    public void testGetAll()
    {
        Map<String, Object> actual = client.execute(
                prepareGet().setUri(uriFor("/v1/service")).build(),
                createJsonResponseHandler(mapCodec, OK.getStatusCode()));
        assertThat(actual.keySet()).containsExactly("environment", "services");
        assertThat(actual.get("environment")).isEqualTo(environment);
        assertThat((Iterable<Object>) actual.get("services")).isEmpty();
    }

    @Test
    public void testProxyGetByType()
    {
        Service proxyStorageService = new Service(Id.random(), Id.random(), "storage", "general", "loc", ImmutableMap.of("key", "5"));
        when(proxyStore.get("storage")).thenAnswer((Answer<Stream<Service>>) invocation -> Stream.of(proxyStorageService));
        when(configStore.get(any(String.class))).thenAnswer((Answer<Stream<Service>>) invocation -> Stream.of());

        Map<String, Object> actual = client.execute(
                prepareGet().setUri(uriFor("/v1/service/storage")).build(),
                createJsonResponseHandler(mapCodec, OK.getStatusCode()));
        assertThat(actual).isEqualTo(ImmutableMap.of(
                "environment", environment,
                "services", ImmutableList.of(
                        toServiceRepresentation(proxyStorageService)
                )));

        actual = client.execute(
                prepareGet().setUri(uriFor("/v1/service/web")).build(),
                createJsonResponseHandler(mapCodec, OK.getStatusCode()));
        assertThat(actual).isEqualTo(ImmutableMap.of(
                "environment", environment,
                "services", ImmutableList.of()));
    }

    @Test
    public void testProxyGetByTypeAndPool()
    {
        Service proxyStorageService = new Service(Id.random(), Id.random(), "storage", "alpha", "loc", ImmutableMap.of("key", "5"));
        when(proxyStore.get("storage", "alpha")).thenAnswer((Answer<Stream<Service>>) invocation -> Stream.of(proxyStorageService));
        when(configStore.get(any(String.class), any(String.class))).thenAnswer((Answer<Stream<Service>>) invocation -> Stream.of());

        Map<String, Object> actual = client.execute(
                prepareGet().setUri(uriFor("/v1/service/storage/alpha")).build(),
                createJsonResponseHandler(mapCodec, OK.getStatusCode()));
        assertThat(actual).isEqualTo(ImmutableMap.of(
                "environment", environment,
                "services", ImmutableList.of(
                        toServiceRepresentation(proxyStorageService)
                )));

        actual = client.execute(
                prepareGet().setUri(uriFor("/v1/service/storage/beta")).build(),
                createJsonResponseHandler(mapCodec, OK.getStatusCode()));
        assertThat(actual).isEqualTo(ImmutableMap.of(
                "environment", environment,
                "services", ImmutableList.of()));

        actual = client.execute(
                prepareGet().setUri(uriFor("/v1/service/storage/unknown")).build(),
                createJsonResponseHandler(mapCodec, OK.getStatusCode()));
        assertThat(actual).isEqualTo(ImmutableMap.of(
                "environment", environment,
                "services", ImmutableList.of()));
    }

    @Test
    public void testProxyGetAll()
    {
        final Service proxyStorageService = new Service(Id.random(), Id.random(), "storage", "alpha", "loc", ImmutableMap.of("key", "5"));
        when(proxyStore.filterAndGetAll(any(Iterable.class))).thenAnswer(invocationOnMock -> Iterables.concat(ImmutableSet.of(proxyStorageService),
                (Iterable<Service>) invocationOnMock.getArguments()[0]));
        when(configStore.getAll()).thenAnswer((Answer<Stream<Service>>) invocation -> Stream.of());

        Map<String, Object> actual = client.execute(
                prepareGet().setUri(uriFor("/v1/service")).build(),
                createJsonResponseHandler(mapCodec, OK.getStatusCode()));
        assertThat(actual.keySet()).containsExactly("environment", "services");
        assertThat(actual.get("environment")).isEqualTo(environment);
        assertThat((Iterable<Object>) actual.get("services")).containsExactlyInAnyOrder(
                toServiceRepresentation(proxyStorageService)
        );
    }

    @Test
    public void testConfigGetByType()
    {
        Service configStorageService = new Service(Id.random(), Id.random(), "storage", "general", "loc", ImmutableMap.of("key", "5"));
        when(proxyStore.get(any(String.class))).thenReturn(null);
        when(configStore.get("storage")).thenAnswer((Answer<Stream<Service>>) invocation -> Stream.of(configStorageService));

        Map<String, Object> actual = client.execute(
                prepareGet().setUri(uriFor("/v1/service/storage")).build(),
                createJsonResponseHandler(mapCodec, OK.getStatusCode()));
        assertThat(actual.keySet()).containsExactly("environment", "services");
        assertThat(actual.get("environment")).isEqualTo(environment);
        assertThat((Iterable<Object>) actual.get("services")).containsExactlyInAnyOrder(
                toServiceRepresentation(configStorageService)
        );

        actual = client.execute(
                prepareGet().setUri(uriFor("/v1/service/web")).build(),
                createJsonResponseHandler(mapCodec, OK.getStatusCode()));
        assertThat(actual).isEqualTo(ImmutableMap.of(
                "environment", environment,
                "services", ImmutableList.of(
                )));
    }

    @Test
    public void testConfigGetByTypeAndPool()
    {
        Service configStorageService = new Service(Id.random(), Id.random(), "storage", "alpha", "loc", ImmutableMap.of("key", "5"));
        when(proxyStore.get(any(String.class), any(String.class))).thenReturn(null);
        when(configStore.get("storage", "alpha")).thenReturn(Stream.of(configStorageService));

        Map<String, Object> actual = client.execute(
                prepareGet().setUri(uriFor("/v1/service/storage/alpha")).build(),
                createJsonResponseHandler(mapCodec, OK.getStatusCode()));
        assertThat(actual.keySet()).containsExactly("environment", "services");
        assertThat(actual.get("environment")).isEqualTo(environment);
        assertThat((Iterable<Object>) actual.get("services")).containsExactlyInAnyOrder(
                toServiceRepresentation(configStorageService)
        );

        actual = client.execute(
                prepareGet().setUri(uriFor("/v1/service/storage/beta")).build(),
                createJsonResponseHandler(mapCodec, OK.getStatusCode()));
        assertThat(actual).isEqualTo(ImmutableMap.of(
                "environment", environment,
                "services", ImmutableList.of(
                )));

        actual = client.execute(
                prepareGet().setUri(uriFor("/v1/service/storage/unknown")).build(),
                createJsonResponseHandler(mapCodec, OK.getStatusCode()));
        assertThat(actual).isEqualTo(ImmutableMap.of(
                "environment", environment,
                "services", ImmutableList.of()));
    }

    @Test
    public void testConfigGetAll()
    {
        final Service proxyStorageService = new Service(Id.random(), Id.random(), "storage", "alpha", "loc", ImmutableMap.of("key", "5"));
        when(proxyStore.filterAndGetAll(any(Iterable.class))).thenAnswer(invocationOnMock -> invocationOnMock.getArguments()[0]);
        when(configStore.getAll()).thenAnswer((Answer<Stream<Service>>) invocation -> Stream.of(proxyStorageService));

        Map<String, Object> actual = client.execute(
                prepareGet().setUri(uriFor("/v1/service")).build(),
                createJsonResponseHandler(mapCodec, OK.getStatusCode()));
        assertThat(actual.keySet()).containsExactly("environment", "services");
        assertThat(actual.get("environment")).isEqualTo(environment);
        assertThat((Iterable<Object>) actual.get("services")).containsExactlyInAnyOrder(
                toServiceRepresentation(proxyStorageService)
        );
    }

    private static Map<String, Object> toServiceRepresentation(Service service)
    {
        return ImmutableMap.<String, Object>builder()
                .put("id", service.getId().toString())
                .put("nodeId", service.getNodeId().toString())
                .put("type", service.getType())
                .put("pool", service.getPool())
                .put("location", service.getLocation())
                .put("properties", service.getProperties())
                .build();
    }

    private URI uriFor(String path)
    {
        return server.getBaseUrl().resolve(path);
    }
}
