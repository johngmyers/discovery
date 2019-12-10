package com.proofpoint.discovery;

import com.google.common.collect.Sets;
import com.proofpoint.audit.AuditLogger;
import com.proofpoint.discovery.client.ServiceDescriptor;
import com.proofpoint.discovery.client.ServiceInventoryConfig;
import com.proofpoint.discovery.client.ServiceSelector;
import com.proofpoint.log.Logger;
import com.proofpoint.units.Duration;

import javax.annotation.Nullable;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.ForbiddenException;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.proofpoint.discovery.AuthAuditRecord.auditRecord;
import static java.util.Objects.requireNonNull;

public class IpHostnameAuthManager implements AuthManager
{
    private static final Logger logger = Logger.get(IpHostnameAuthManager.class);
    private final DynamicStore dynamicStore;
    private final ServiceSelector discoverySelector;
    private final Duration updateInterval;
    private final Set<InetAddress> discoveryAddrs = Sets.newConcurrentHashSet();
    private final ScheduledExecutorService executor;
    private final AuditLogger<AuthAuditRecord> auditLogger;
    private Future<?> future;

    @Inject
    public IpHostnameAuthManager(DynamicStore dynamicStore,
            ServiceSelector discoverySelector,
            ServiceInventoryConfig config,
            @ForAuthManager ScheduledExecutorService executor,
            AuditLogger<AuthAuditRecord> auditLogger)
    {
        this.dynamicStore = requireNonNull(dynamicStore, "dynamicStore is null");
        this.discoverySelector = requireNonNull(discoverySelector, "discoverySelector is null");
        this.updateInterval = requireNonNull(config, "config is null").getUpdateInterval();
        this.executor = requireNonNull(executor, "executor is null");
        this.auditLogger = requireNonNull(auditLogger, "auditLogger is null");
    }

    @PreDestroy
    private void preDestroy()
    {
        if (future != null) {
            future.cancel(true);
        }
        executor.shutdownNow();
        future = null;
    }

    @PostConstruct
    private void startDiscoveryRefresh()
    {
        if (future == null) {
            future = executor.scheduleWithFixedDelay(() -> {
                try {
                    updateDiscoveryAddrs(discoverySelector.selectAllServices());
                }
                catch (Exception e) {
                    logger.warn(e, "Unable to refresh discovery servers. No replication will be permitted");
                }
            }, 0, updateInterval.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    private void updateDiscoveryAddrs(List<ServiceDescriptor> descriptors)
    {
        Set<InetAddress> otherDiscoveryHosts = descriptors.stream()
                .map(d -> d.getProperties().get("http"))
                .map(URI::create)
                .map(u -> {
                    try {
                        return InetAddress.getAllByName(u.getHost());
                    }
                    catch (UnknownHostException e) {
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .flatMap(Arrays::stream)
                .collect(Collectors.toSet());
        discoveryAddrs.removeIf(entry -> !otherDiscoveryHosts.contains(entry));
        discoveryAddrs.addAll(otherDiscoveryHosts);
    }

    @Override
    public void checkAuthAnnounce(Id<Node> nodeId, DynamicAnnouncement announcement, HttpServletRequest request)
    {
        try {
            InetAddress requesterAddress = InetAddress.getByName(request.getRemoteAddr());
            String nodeAnnouncer = dynamicStore.getAnnouncer(nodeId);
            if (mismatchedAnnouncer(nodeAnnouncer, requesterAddress)) {
                //NodeId was previously announced by a different IP
                auditLogger.audit(auditRecord("IP %s tried to re-announce node %s owned by IP %s", requesterAddress.getHostAddress(), nodeId.toString(), nodeAnnouncer));
                throw new ForbiddenException();
            }
            Set<DynamicServiceAnnouncement> newAnnouncements = announcement.getServiceAnnouncements();
            for (DynamicServiceAnnouncement newAnnouncement : newAnnouncements) {
                Map<String, String> properties = newAnnouncement.getProperties();
                if (invalidHostnameIp(properties.get("http"), requesterAddress) ||
                        invalidHostnameIp(properties.get("https"), requesterAddress) ||
                        invalidHostnameIp(properties.get("admin"), requesterAddress)) {
                    //Service is announcing a host with a different set of IPs
                    throw new ForbiddenException();
                }
            }
        }
        catch (UnknownHostException e) {
            //Unable to validate an IP or look up a hostname
            auditLogger.audit(auditRecord(e.getMessage()));
            throw new ForbiddenException();
        }
    }

    @Override
    public void checkAuthDelete(Id<Node> nodeId, HttpServletRequest request)
    {
        try {
            InetAddress requesterAddress = InetAddress.getByName(request.getRemoteAddr());
            String nodeAnnouncer = dynamicStore.getAnnouncer(nodeId);
            if (mismatchedAnnouncer(nodeAnnouncer, requesterAddress)) {
                auditLogger.audit(auditRecord("IP %s tried to delete node %s owned by IP %s", requesterAddress.getHostAddress(), nodeId.toString(), nodeAnnouncer));
                throw new ForbiddenException();
            }
        }
        catch (UnknownHostException e) {
            auditLogger.audit(auditRecord(e.getMessage()));
            throw new ForbiddenException();
        }
    }

    @Override
    public void checkAuthReplicate(HttpServletRequest request)
    {
        try {
            InetAddress requesterAddress = InetAddress.getByName(request.getRemoteAddr());
            if (!discoveryAddrs.contains(requesterAddress)) {
                auditLogger.audit(auditRecord("IP %s tried to replicate as Discovery", requesterAddress.getHostAddress()));
                throw new ForbiddenException();
            }
        }
        catch (UnknownHostException e) {
            auditLogger.audit(auditRecord(e.getMessage()));
            throw new ForbiddenException();
        }
    }

    private boolean mismatchedAnnouncer(@Nullable String nodeAnnouncer, InetAddress requesterAddr)
            throws UnknownHostException
    {
        return nodeAnnouncer != null && !requesterAddr.equals(InetAddress.getByName(nodeAnnouncer));
    }

    private boolean invalidHostnameIp(String hostname, InetAddress requesterAddress)
    {
        if (hostname == null) {
            return false;
        }
        try {
            URI hostUri = URI.create(hostname);
            InetAddress[] hostIps = InetAddress.getAllByName(hostUri.getHost());
            if (!Arrays.asList(hostIps).contains(requesterAddress)) {
                auditLogger.audit(auditRecord("IP %s tried to announce other host %s", requesterAddress.getHostAddress(), hostUri.getHost()));
                return true;
            }
            return false;
        }
        catch (UnknownHostException e) {
            auditLogger.audit(auditRecord(e.getMessage()));
            return true;
        }
    }
}
