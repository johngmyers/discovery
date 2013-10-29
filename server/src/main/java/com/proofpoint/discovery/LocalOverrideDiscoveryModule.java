package com.proofpoint.discovery;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.proofpoint.discovery.client.DiscoveryLookupClient;
import com.proofpoint.discovery.client.announce.DiscoveryAnnouncementClient;

public class LocalOverrideDiscoveryModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(DiscoveryLookupClient.class).to(LocalDiscoveryLookupClient.class).in(Scopes.SINGLETON);
        binder.bind(DiscoveryAnnouncementClient.class).to(LocalDiscoveryAnnouncement.class).in(Scopes.SINGLETON);
    }
}
