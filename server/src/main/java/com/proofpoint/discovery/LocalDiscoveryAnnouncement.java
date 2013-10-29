package com.proofpoint.discovery;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import com.proofpoint.discovery.client.DiscoveryException;
import com.proofpoint.discovery.client.announce.DiscoveryAnnouncementClient;
import com.proofpoint.discovery.client.announce.ServiceAnnouncement;
import com.proofpoint.node.NodeInfo;
import com.proofpoint.units.Duration;

import javax.ws.rs.core.Response;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;

public class LocalDiscoveryAnnouncement
    implements DiscoveryAnnouncementClient
{
    private final DynamicAnnouncementResource dynamicAnnouncementResource;
    private final NodeInfo nodeInfo;
    private final Id<Node> nodeId;

    @Inject
    public LocalDiscoveryAnnouncement(DynamicAnnouncementResource dynamicAnnouncementResource, NodeInfo nodeInfo)
    {
        this.dynamicAnnouncementResource = checkNotNull(dynamicAnnouncementResource, "dynamicAnnouncementResource is null");
        this.nodeInfo = checkNotNull(nodeInfo, "nodeInfo is null");
        nodeId = Id.valueOf(nodeInfo.getNodeId());
    }

    @Override
    public ListenableFuture<Duration> announce(Set<ServiceAnnouncement> services)
    {
        ImmutableSet.Builder<DynamicServiceAnnouncement> builder = ImmutableSet.builder();
        for (ServiceAnnouncement service : services) {
            builder.add(new DynamicServiceAnnouncement(Id.<Service>valueOf(service.getId()), service.getType(), service.getProperties()));
        }

        DynamicAnnouncement announcement = new DynamicAnnouncement(nodeInfo.getEnvironment(), nodeInfo.getPool(), nodeInfo.getLocation(), builder.build());
        Response response = dynamicAnnouncementResource.put(nodeId, announcement);
        if (response.getStatus() / 100 == 2) {
            return immediateFuture(DEFAULT_DELAY);
        }
        return immediateFailedFuture(new DiscoveryException(String.format("Announcement failed with status code %s", response.getStatus())));
    }

    @Override
    public ListenableFuture<Void> unannounce()
    {
        dynamicAnnouncementResource.delete(nodeId);
        return immediateFuture(null);
    }
}
