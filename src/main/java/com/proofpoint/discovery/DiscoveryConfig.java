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

import com.google.common.collect.ForwardingSet;
import com.google.common.collect.ImmutableSet;
import com.proofpoint.configuration.Config;
import com.proofpoint.configuration.ConfigDescription;
import com.proofpoint.units.Duration;

import javax.validation.constraints.AssertTrue;
import javax.validation.constraints.NotNull;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class DiscoveryConfig
{
    private boolean dynamicEnabled = true;
    private Duration maxAge = new Duration(90, TimeUnit.SECONDS);
    private String mapTarget = "general";
    private ReplicationMode generalPoolLegacyReplicationMode = ReplicationMode.PHASE_ONE;
    private StringSet proxyProxiedTypes = StringSet.of();
    private String proxyEnvironment = null;
    private UriSet proxyUris = UriSet.of();

    public boolean isDynamicEnabled()
    {
        return dynamicEnabled;
    }

    @Config("discovery.dynamic.enabled")
    @ConfigDescription("Whether dynamic announcements are enabled")
    public DiscoveryConfig setDynamicEnabled(boolean dynamicEnabled)
    {
        this.dynamicEnabled = dynamicEnabled;
        return this;
    }

    @NotNull
    public Duration getMaxAge()
    {
        return maxAge;
    }

    @Config("discovery.max-age")
    @ConfigDescription("Dynamic announcement expiration")
    public DiscoveryConfig setMaxAge(Duration maxAge)
    {
        this.maxAge = maxAge;
        return this;
    }

    public String getGeneralPoolMapTarget()
    {
        return mapTarget;
    }

    @Config("reporting.tag.datacenter")
    public DiscoveryConfig setGeneralPoolMapTarget(String mapTarget)
    {
        this.mapTarget = mapTarget;
        return this;
    }

    public ReplicationMode getGeneralPoolLegacyReplicationMode()
    {
        return generalPoolLegacyReplicationMode;
    }

    @Config("discovery.general-pool.legacy-replication-mode")
    public DiscoveryConfig setGeneralPoolLegacyReplicationMode(ReplicationMode mode)
    {
        generalPoolLegacyReplicationMode = mode;
        return this;
    }

    public StringSet getProxyProxiedTypes()
    {
        return proxyProxiedTypes;
    }

    @Config("discovery.proxy.proxied-types")
    @ConfigDescription("Service types to proxy (test environments only)")
    public DiscoveryConfig setProxyProxiedTypes(StringSet proxyProxiedTypes)
    {
        this.proxyProxiedTypes = proxyProxiedTypes;
        return this;
    }

    public String getProxyEnvironment()
    {
        return proxyEnvironment;
    }

    @Config("discovery.proxy.environment")
    @ConfigDescription("Environment to proxy to (test environments only)")
    public DiscoveryConfig setProxyEnvironment(String proxyEnvironment)
    {
        this.proxyEnvironment = proxyEnvironment;
        return this;
    }

    public UriSet getProxyUris()
    {
        return proxyUris;
    }

    @Config("discovery.proxy.uri")
    @ConfigDescription("Discovery servers to proxy to (test environments only)")
    public DiscoveryConfig setProxyUris(UriSet proxyUris)
    {
        this.proxyUris = proxyUris;
        return this;
    }

    @AssertTrue(message = "discovery.proxy.environment specified if and only if any proxy types")
    public boolean isProxyTypeAndEnvironment()
    {
        return proxyProxiedTypes.isEmpty() == (proxyEnvironment == null);
    }

    @AssertTrue(message = "discovery.proxy.uri specified if and only if any proxy types")
    public boolean isProxyTypeAndUri()
    {
        return proxyProxiedTypes.isEmpty() == proxyUris.isEmpty();
    }

    public static final class StringSet extends ForwardingSet<String>
    {
        private final Set<String> delegate;

        private StringSet(Set<String> delegate)
        {
            this.delegate = ImmutableSet.copyOf(delegate);
        }

        public static StringSet of(String... strings)
        {
            return new StringSet(ImmutableSet.copyOf(strings));
        }

        public static StringSet valueOf(String string)
        {
            return of(string.split("\\s*,\\s*"));
        }

        @Override
        protected Set<String> delegate()
        {
            return delegate;
        }
    }

    public static final class UriSet extends ForwardingSet<URI>
    {
        private final Set<URI> delegate;

        private UriSet(Set<URI> delegate)
        {
            this.delegate = ImmutableSet.copyOf(delegate);
        }

        public static UriSet of(URI... uris)
        {
            return new UriSet(ImmutableSet.copyOf(uris));
        }

        public static UriSet valueOf(String string)
        {
            List<URI> uris = Arrays.stream(string.split("\\s*,\\s*"))
                    .map(URI::create)
                    .collect(Collectors.toList());
            return new UriSet(ImmutableSet.copyOf(uris));
        }

        @Override
        protected Set<URI> delegate()
        {
            return delegate;
        }
    }

    public enum ReplicationMode
    {
        PHASE_ONE, PHASE_TWO, PHASE_THREE;

        public static ReplicationMode fromString(String mode)
        {
            return ReplicationMode.valueOf(mode.toUpperCase());

        }
    }}
