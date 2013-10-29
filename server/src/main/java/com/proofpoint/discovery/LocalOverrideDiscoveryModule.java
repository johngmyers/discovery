package com.proofpoint.discovery;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.proofpoint.discovery.client.ServiceType;
import com.proofpoint.http.client.balancing.HttpServiceBalancer;
import com.proofpoint.http.client.balancing.HttpServiceBalancerImpl;
import com.proofpoint.http.client.balancing.HttpServiceBalancerStats;
import com.proofpoint.http.server.HttpServerInfo;
import com.proofpoint.reporting.Key;
import com.proofpoint.stats.CounterStat;
import com.proofpoint.stats.TimeStat;

import java.net.URI;

public class LocalOverrideDiscoveryModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
    }

    @Provides
    @Singleton
    @ServiceType("discovery")
    public HttpServiceBalancer createHttpServiceBalancer(HttpServerInfo httpServerInfo)
    {
        HttpServiceBalancerImpl balancer = new HttpServiceBalancerImpl("discovery", new IgnoreHttpServiceBalancerStats());
        balancer.updateHttpUris(ImmutableSet.of(httpServerInfo.getHttpUri()));
        return balancer;
    }

    @Provides
    @Singleton
    @ServiceType("discovery")
    public HttpServiceBalancerImpl createHttpServiceBalancerImpl()
    {
        return new HttpServiceBalancerImpl("unused discovery balancer implementation", new IgnoreHttpServiceBalancerStats());
    }

    private static class IgnoreHttpServiceBalancerStats implements HttpServiceBalancerStats
    {
        @Override
        public CounterStat failure(@Key("targetUri") URI uri, @Key("failure") String failureCategory)
        {
            return new CounterStat();
        }

        @Override
        public TimeStat responseTime(@Key("targetUri") URI uri, @Key("status") Status status)
        {
            return new TimeStat();
        }
    }
}
