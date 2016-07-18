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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.proofpoint.bootstrap.LifeCycleManager;
import com.proofpoint.discovery.client.DiscoveryModule;
import com.proofpoint.discovery.client.ServiceDescriptor;
import com.proofpoint.discovery.client.ServiceSelector;
import com.proofpoint.discovery.client.announce.DiscoveryAnnouncementClient;
import com.proofpoint.discovery.client.announce.ServiceAnnouncement;
import com.proofpoint.event.client.InMemoryEventModule;
import com.proofpoint.http.client.HttpClient;
import com.proofpoint.http.client.jetty.JettyHttpClient;
import com.proofpoint.http.server.testing.TestingHttpServer;
import com.proofpoint.http.server.testing.TestingHttpServerModule;
import com.proofpoint.json.JsonModule;
import com.proofpoint.node.NodeInfo;
import com.proofpoint.node.testing.TestingNodeModule;
import com.proofpoint.reporting.ReportingModule;
import com.proofpoint.testing.Closeables;
import org.iq80.leveldb.util.FileUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.weakref.jmx.guice.MBeanModule;
import org.weakref.jmx.testing.TestingMBeanModule;

import java.io.File;
import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.proofpoint.bootstrap.Bootstrap.bootstrapTest;
import static com.proofpoint.discovery.client.DiscoveryBinder.discoveryBinder;
import static com.proofpoint.discovery.client.ServiceTypes.serviceType;
import static com.proofpoint.jaxrs.JaxrsModule.explicitJaxrsModule;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestDiscoveryServer
{
    private final HttpClient client = new JettyHttpClient();

    private TestingHttpServer server;
    private File tempDir;
    private Set<LifeCycleManager> lifeCycleManagers;

    @BeforeMethod
    public void setup()
            throws Exception
    {
        tempDir = Files.createTempDir();

        // start server
        Map<String, String> serverProperties = ImmutableMap.<String, String>builder()
                .put("static.db.location", tempDir.getAbsolutePath())
                .build();

        Injector serverInjector = bootstrapTest()
                .withModules(
                        new MBeanModule(),
                        new TestingNodeModule("testing"),
                        new ReportingModule(),
                        new TestingMBeanModule(),
                        new TestingHttpServerModule(),
                        new JsonModule(),
                        explicitJaxrsModule(),
                        new DiscoveryServerModule(),
                        new DiscoveryModule(),
                        new ReportingModule())
                .setRequiredConfigurationProperties(serverProperties)
                .initialize();

        lifeCycleManagers = new HashSet<>();
        lifeCycleManagers.add(serverInjector.getInstance(LifeCycleManager.class));
        server = serverInjector.getInstance(TestingHttpServer.class);
    }

    @AfterMethod
    public void teardown()
            throws Exception
    {
        for (LifeCycleManager lifeCycleManager : lifeCycleManagers) {
            lifeCycleManager.stop();
        }
        FileUtils.deleteRecursively(tempDir);
    }

    @AfterClass(alwaysRun = true)
    public void teardownClass()
    {
        Closeables.closeQuietly(client);
    }

    @Test
    public void testDynamicAnnouncement()
            throws Exception
    {
        // publish announcement
        Map<String, String> announcerProperties = ImmutableMap.<String, String>builder()
            .put("testing.discovery.uri", server.getBaseUrl().toString())
            .build();

        Injector announcerInjector = bootstrapTest()
                .withModules(
                        new TestingNodeModule("testing", "red"),
                        new ReportingModule(),
                        new TestingMBeanModule(),
                        new InMemoryEventModule(),
                        new JsonModule(),
                        new DiscoveryModule()
                )
                .setRequiredConfigurationProperties(announcerProperties)
                .initialize();

        lifeCycleManagers.add(announcerInjector.getInstance(LifeCycleManager.class));

        ServiceAnnouncement announcement = ServiceAnnouncement.serviceAnnouncement("apple")
                .addProperties(ImmutableMap.of("key", "value"))
                .build();

        DiscoveryAnnouncementClient client = announcerInjector.getInstance(DiscoveryAnnouncementClient.class);
        client.announce(ImmutableSet.of(announcement)).get();

        NodeInfo announcerNodeInfo = announcerInjector.getInstance(NodeInfo.class);

        List<ServiceDescriptor> services = selectorFor("apple", "red").selectAllServices();
        assertEquals(services.size(), 1);

        ServiceDescriptor service = services.get(0);
        assertNotNull(service.getId());
        assertEquals(service.getNodeId(), announcerNodeInfo.getNodeId());
        assertEquals(service.getLocation(), announcerNodeInfo.getLocation());
        assertEquals(service.getPool(), announcerNodeInfo.getPool());
        assertEquals(service.getProperties(), announcement.getProperties());


        // ensure that service is no longer visible
        client.unannounce().get();

        assertTrue(selectorFor("apple", "red").selectAllServices().isEmpty());
    }

    private ServiceSelector selectorFor(final String type, String pool)
            throws Exception
    {
        Map<String, String> clientProperties = ImmutableMap.<String, String>builder()
            .put("testing.discovery.uri", server.getBaseUrl().toString())
            .put("discovery.apple.pool", pool)
            .build();

        Injector clientInjector = bootstrapTest()
                .withModules(
                        new TestingNodeModule("testing"),
                        new ReportingModule(),
                        new TestingMBeanModule(),
                        new InMemoryEventModule(),
                        new JsonModule(),
                        new DiscoveryModule(),
                        binder -> discoveryBinder(binder).bindSelector(type)
                )
                .setRequiredConfigurationProperties(clientProperties)
                .initialize();

        lifeCycleManagers.add(clientInjector.getInstance(LifeCycleManager.class));

        return clientInjector.getInstance(Key.get(ServiceSelector.class, serviceType(type)));
    }

    private URI uriFor(String path)
    {
        return server.getBaseUrl().resolve(path);
    }
}
