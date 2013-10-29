package com.proofpoint.discovery;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import com.proofpoint.discovery.client.DiscoveryLookupClient;
import com.proofpoint.discovery.client.ServiceDescriptor;
import com.proofpoint.discovery.client.ServiceDescriptors;
import com.proofpoint.discovery.client.ServiceState;
import com.proofpoint.discovery.client.announce.DiscoveryAnnouncementClient;

import static com.google.common.base.Preconditions.checkNotNull;

public class LocalDiscoveryLookupClient
    implements DiscoveryLookupClient
{
    private final ServiceResource serviceResource;

    @Inject
    public LocalDiscoveryLookupClient(ServiceResource serviceResource)
    {
        this.serviceResource = checkNotNull(serviceResource, "serviceResource is null");
    }

    @Override
    public ListenableFuture<ServiceDescriptors> getServices(String type)
    {
        return convertServices(type, null, serviceResource.getServices(type));
    }

    @Override
    public ListenableFuture<ServiceDescriptors> getServices(String type, String pool)
    {
        return convertServices(type, pool, serviceResource.getServices(type, pool));
    }

    @Override
    public ListenableFuture<ServiceDescriptors> refreshServices(ServiceDescriptors serviceDescriptors)
    {
        String type = serviceDescriptors.getType();
        String pool = serviceDescriptors.getPool();

        Services services;
        if (pool == null) {
            services = serviceResource.getServices(type);
        }
        else {
            services = serviceResource.getServices(type, pool);
        }

        return convertServices(type, pool, services);
    }

    private static ListenableFuture<ServiceDescriptors> convertServices(String type, String pool, Services services)
    {
        Builder<ServiceDescriptor> builder = ImmutableList.builder();
        for (Service service : services.getServices()) {
            builder.add(new ServiceDescriptor(
                    service.getId().get(),
                    service.getNodeId().get().toString(),
                    service.getType(),
                    service.getPool(),
                    service.getLocation(),
                    ServiceState.RUNNING,
                    service.getProperties()
            ));
        }

        return Futures.immediateFuture(new ServiceDescriptors(type, pool, builder.build(), DiscoveryAnnouncementClient.DEFAULT_DELAY, null));
    }
}
