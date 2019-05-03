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
import com.proofpoint.configuration.testing.ConfigAssertions;
import com.proofpoint.discovery.DiscoveryConfig.ReplicationMode;
import com.proofpoint.discovery.DiscoveryConfig.StringSet;
import com.proofpoint.units.Duration;
import org.testng.annotations.Test;

import javax.validation.constraints.AssertTrue;
import javax.validation.constraints.NotNull;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.proofpoint.configuration.testing.ConfigAssertions.assertLegacyEquivalence;
import static com.proofpoint.testing.ValidationAssertions.assertFailsValidation;

public class TestDiscoveryConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(DiscoveryConfig.class)
                .setDynamicEnabled(true)
                .setMaxAge(new Duration(90, TimeUnit.SECONDS))
                .setGeneralPoolMapTarget("general")
                .setGeneralPoolLegacyReplicationMode(ReplicationMode.PHASE_ONE)
                .setProxyProxiedTypes(DiscoveryConfig.StringSet.of())
                .setProxyEnvironment(null)
                .setProxyUris(DiscoveryConfig.UriSet.of()));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("discovery.dynamic.enabled", "false")
                .put("discovery.max-age", "1m")
                .put("reporting.tag.datacenter", "SNV")
                .put("discovery.general-pool.legacy-replication-mode", "phase_two")
                .put("discovery.proxy.proxied-types", "foo  ,  bar")
                .put("discovery.proxy.environment", "pre-release")
                .put("discovery.proxy.uri", "http://10.20.30.40:4111,http://50.60.70.80:9125")
                .build();

        DiscoveryConfig expected = new DiscoveryConfig()
                .setDynamicEnabled(false)
                .setMaxAge(new Duration(1, TimeUnit.MINUTES))
                .setGeneralPoolMapTarget("SNV")
                .setGeneralPoolLegacyReplicationMode(ReplicationMode.PHASE_TWO)
                .setProxyProxiedTypes(DiscoveryConfig.StringSet.of("foo", "bar"))
                .setProxyEnvironment("pre-release")
                .setProxyUris(DiscoveryConfig.UriSet.of(URI.create("http://10.20.30.40:4111"), URI.create("http://50.60.70.80:9125")));

        ConfigAssertions.assertFullMapping(properties, expected);
    }

    @Test
    public void testLegacyProperties()
    {
        assertLegacyEquivalence(DiscoveryConfig.class,
                ImmutableMap.of());
    }

    @Test
    public void testValidatesNotNullDuration()
    {
        DiscoveryConfig config = new DiscoveryConfig().setMaxAge(null);

        assertFailsValidation(config, "maxAge", "may not be null", NotNull.class);
    }

    @Test
    public void testProxyMissingEnvironment()
    {
        DiscoveryConfig config = new DiscoveryConfig()
                .setProxyProxiedTypes(StringSet.of("foo"))
                .setProxyUris(DiscoveryConfig.UriSet.of(URI.create("http://10.20.30.40:4111")));
        assertFailsValidation(config, "proxyTypeAndEnvironment", "discovery.proxy.environment specified if and only if any proxy types",
                AssertTrue.class);
    }

    @Test
    public void testProxyEnvironment()
    {
        DiscoveryConfig config = new DiscoveryConfig().setProxyEnvironment("pre-release");
        assertFailsValidation(config, "proxyTypeAndEnvironment", "discovery.proxy.environment specified if and only if any proxy types",
                AssertTrue.class);
    }

    @Test
    public void testProxyMissingUri()
    {
        DiscoveryConfig config = new DiscoveryConfig().setProxyProxiedTypes(StringSet.of("foo")).setProxyEnvironment("pre-release");
        assertFailsValidation(config, "proxyTypeAndUri", "discovery.proxy.uri specified if and only if any proxy types",
                AssertTrue.class);
    }

    @Test
    public void testProxyUri()
    {
        DiscoveryConfig config = new DiscoveryConfig().setProxyUris(DiscoveryConfig.UriSet.of(URI.create("http://10.20.30.40:4111")));
        assertFailsValidation(config, "proxyTypeAndUri", "discovery.proxy.uri specified if and only if any proxy types",
                AssertTrue.class);
    }
}
