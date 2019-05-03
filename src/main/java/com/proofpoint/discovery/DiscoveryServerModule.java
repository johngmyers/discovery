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
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.proofpoint.configuration.AbstractConfigurationAwareModule;
import com.proofpoint.discovery.client.ServiceDescriptor;
import com.proofpoint.discovery.client.ServiceInventory;
import com.proofpoint.discovery.client.ServiceSelector;
import com.proofpoint.discovery.store.DistributedStore;
import com.proofpoint.discovery.store.InMemoryStore;
import com.proofpoint.discovery.store.ReplicatedStoreModule;
import com.proofpoint.node.NodeInfo;

import javax.inject.Singleton;
import java.util.List;

import static com.proofpoint.configuration.ConfigBinder.bindConfig;
import static com.proofpoint.discovery.client.DiscoveryBinder.discoveryBinder;
import static com.proofpoint.http.client.HttpClientBinder.httpClientBinder;
import static com.proofpoint.jaxrs.JaxrsBinder.jaxrsBinder;

public class DiscoveryServerModule
        extends AbstractConfigurationAwareModule
{
    @Override
    public void setup(Binder binder)
    {
        bindConfig(binder).bind(DiscoveryConfig.class);
        jaxrsBinder(binder).bind(ServiceResource.class).withApplicationPrefix();
        binder.bind(InitializationTracker.class).in(Scopes.SINGLETON);

        discoveryBinder(binder).bindHttpAnnouncement("discovery");

        DiscoveryConfig discoveryConfig = buildConfigObject(DiscoveryConfig.class);

        // dynamic announcements
        if (discoveryConfig.isDynamicEnabled()) {
            jaxrsBinder(binder).bind(DynamicAnnouncementResource.class).withApplicationPrefix();
            binder.bind(DynamicStore.class).to(Key.get(DistributedStore.class, ForDynamicStore.class)).in(Scopes.SINGLETON);
            binder.install(new ReplicatedStoreModule("dynamic", ForDynamicStore.class, InMemoryStore.class));
        } else {
            binder.bind(DynamicStore.class).to(NullDynamicStore.class).in(Scopes.SINGLETON);
        }

        // config-based static announcements
        binder.bind(ConfigStore.class).in(Scopes.SINGLETON);
        bindConfig(binder).bind(ConfigStoreConfig.class);

        // proxy announcements
        if (!discoveryConfig.getProxyUris().isEmpty()) {
            httpClientBinder(binder).bindBalancingHttpClient("discovery.proxy", ForProxyStore.class, discoveryConfig.getProxyUris());
        }
        binder.bind(ProxyStore.class).in(Scopes.SINGLETON);
    }
}
