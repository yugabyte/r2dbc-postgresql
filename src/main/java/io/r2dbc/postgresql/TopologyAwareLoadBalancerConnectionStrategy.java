package io.r2dbc.postgresql;

import io.r2dbc.postgresql.api.PostgresqlResult;
import io.r2dbc.postgresql.client.Client;
import io.r2dbc.postgresql.client.ConnectionSettings;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.net.InetSocketAddress;
import java.util.*;

public class TopologyAwareLoadBalancerConnectionStrategy extends UniformLoadBalancerConnectionStrategy{

    protected final ConnectionFunction connectionFunction;
    private final ConnectionSettings connectionSettings;
    private final PostgresqlConnectionConfiguration configuration;
    private final String placements;
    private final Map<Integer, Set<CloudPlacement>> allowedPlacements = new HashMap<>();
    private final Map<Integer, List<String>> fallbackPrivateIPs = new HashMap<>();
    private final Map<Integer, List<String>> fallbackPublicIPs = new HashMap<>();
    private final int PRIMARY_PLACEMENTS = 1;
    private final int FIRST_FALLBACK = 2;
    private final int REST_OF_CLUSTER = -1;
    public static final int MAX_PREFERENCE_VALUE = 10;
    private static final Logger LOGGER = Loggers.getLogger(TopologyAwareLoadBalancerConnectionStrategy.class.getName());

    public TopologyAwareLoadBalancerConnectionStrategy(ConnectionFunction connectionFunction, PostgresqlConnectionConfiguration configuration, String placementvalues, ConnectionSettings settings, int refreshListSeconds) {
        super(connectionFunction,configuration,settings, refreshListSeconds);
        placements = placementvalues;
        this.connectionFunction = connectionFunction;
        this.configuration = configuration;
        this.connectionSettings = settings;
        this.refreshListSeconds = refreshListSeconds >= 0 && refreshListSeconds <= 600 ?
                refreshListSeconds : 300;
        parseGeoLocations();
        LOGGER.debug("TopologyAwareLoadBalancerConnectionStrategy initialized with placements: {}, refresh interval: {}s", placementvalues, this.refreshListSeconds);
    }

    private void populatePlacementSet(String placements, Set<CloudPlacement> allowedPlacements) {
        String[] pStrings = placements.split(",");
        for (String pl : pStrings) {
            String[] placementParts = pl.split("\\.");
            if (placementParts.length != 3 || placementParts[0].equals("*") || placementParts[1].equals("*")) {
                // Return an error so the user takes corrective action.
                throw new IllegalArgumentException("Malformed topology-keys property value: " + pl);
            }
            CloudPlacement cp = new CloudPlacement(
                    placementParts[0], placementParts[1], placementParts[2]);
            allowedPlacements.add(cp);
        }
    }

    private void parseGeoLocations() {
        LOGGER.trace("Parsing topology-keys: {}", placements);
        String[] values = placements.split(",");
        for (String value : values) {
            String[] v = value.split(":");
            if (v.length > 2 || value.endsWith(":")) {
                throw new IllegalArgumentException("Invalid value part for topology-keys property : " + value);
            }
            if (v.length == 1) {
                Set<CloudPlacement> primary = allowedPlacements.computeIfAbsent(PRIMARY_PLACEMENTS, k -> new HashSet<>());
                populatePlacementSet(v[0], primary);
            } else {
                int pref = Integer.valueOf(v[1]);
                if (pref == 1) {
                    Set<CloudPlacement> primary = allowedPlacements.get(PRIMARY_PLACEMENTS);
                    if (primary == null) {
                        primary = new HashSet<>();
                        allowedPlacements.put(PRIMARY_PLACEMENTS, primary);
                    }
                    populatePlacementSet(v[0], primary);
                } else if (pref > 1 && pref <= MAX_PREFERENCE_VALUE) {
                    Set<CloudPlacement> fallbackPlacements = allowedPlacements.get(pref);
                    if (fallbackPlacements == null) {
                        fallbackPlacements = new HashSet<>();
                        allowedPlacements.put(pref, fallbackPlacements);
                    }
                    populatePlacementSet(v[0], fallbackPlacements);
                } else {
                    throw new IllegalArgumentException("Invalid preference value for property topology-keys: " + value);
                }
            }
        }
        LOGGER.debug("Parsed placement preferences: {}", allowedPlacements);
    }

    @Override
    protected List<String> getCurrentServers(PostgresqlConnection controlConnection){
        LOGGER.debug("Querying yb_servers() with topology-aware strategy. Placements: {}", placements);
        currentPublicIps.clear();
        hostToPriorityMap.clear();
        List <String> allPrivateIPs = new ArrayList<>();
        List <String> allPublicIPs = new ArrayList<>();
        Flux<PostgresqlResult> Results = controlConnection.createStatement("Select * from yb_servers()").execute();
        List<String> privateHosts = Results.flatMap(result -> result.map((row, rowMetadata) -> {
                    String host = row.get("host", String.class);
                    String cloud = row.get("cloud", String.class);
                    String region = row.get("region", String.class);
                    String zone = row.get("zone", String.class);
                    updatePriorityMap(host, cloud, region, zone);
                    CloudPlacement cp = new CloudPlacement(cloud, region, zone);
                    if (cp.isContainedIn(allowedPlacements.get(PRIMARY_PLACEMENTS))){
                       return host;
                    }
                    return "";
                }))
                .collectList()
                .block();

        privateHosts.removeAll(Arrays.asList("", null));
        LOGGER.trace("Primary placement private hosts: {}", privateHosts);
        allPrivateIPs.addAll(privateHosts);

        currentPublicIps = Results.flatMap(result -> result.map((row, rowMetadata) -> {
            String host = row.get("public_ip", String.class);
            String cloud = row.get("cloud", String.class);
            String region = row.get("region", String.class);
            String zone = row.get("zone", String.class);
            CloudPlacement cp = new CloudPlacement(cloud, region, zone);
            if (cp.isContainedIn(allowedPlacements.get(PRIMARY_PLACEMENTS))){
                return host;
            }
            return "";
        }))
                .collectList()
                .block();
        currentPublicIps.removeAll(Arrays.asList("", null));
        LOGGER.trace("Primary placement public IPs: {}", currentPublicIps);
        allPublicIPs.addAll(currentPublicIps);

        for (Map.Entry<Integer, Set<CloudPlacement>> allowedCPs : allowedPlacements.entrySet()) {
            List<String> privateIPs = Results.flatMap(result -> result.map((row, rowMetadata) -> {
                        String host = row.get("host", String.class);
                        String cloud = row.get("cloud", String.class);
                        String region = row.get("region", String.class);
                        String zone = row.get("zone", String.class);
                        updatePriorityMap(host, cloud, region, zone);
                        CloudPlacement cp = new CloudPlacement(cloud, region, zone);
                        if (cp.isContainedIn(allowedCPs.getValue())){
                            return host;
                        }
                        return "";
                    }))
                    .collectList()
                    .block();

            privateIPs.removeAll(Arrays.asList("", null));
            fallbackPrivateIPs.put(allowedCPs.getKey(), privateIPs);
            allPrivateIPs.addAll(privateIPs);

            List<String> publicIPs = Results.flatMap(result -> result.map((row, rowMetadata) -> {
                String host = row.get("public_ip", String.class);
                String cloud = row.get("cloud", String.class);
                String region = row.get("region", String.class);
                String zone = row.get("zone", String.class);
                CloudPlacement cp = new CloudPlacement(cloud, region, zone);
                if (cp.isContainedIn(allowedCPs.getValue())){
                    return host;
                }
                return "";
            }))
                    .collectList()
                    .block();

            publicIPs.removeAll(Arrays.asList("", null));
            fallbackPublicIPs.put(allowedCPs.getKey(), publicIPs);
            allPublicIPs.addAll(publicIPs);
            LOGGER.trace("Preference level {} - private IPs: {}, public IPs: {}", allowedCPs.getKey(), privateIPs, publicIPs);
        }

        // For rest of the cluster

        List<String> restprivateIPs = Results.flatMap(result -> result.map((row, rowMetadata) -> {
            String host = row.get("host", String.class);
            String cloud = row.get("cloud", String.class);
            String region = row.get("region", String.class);
            String zone = row.get("zone", String.class);
            updatePriorityMap(host, cloud, region, zone);
            if(!allPrivateIPs.contains(host)){
                return host;
            }
            return "";
        }))
                .collectList()
                .block();

        restprivateIPs.removeAll(Arrays.asList("", null));
        fallbackPrivateIPs.put(REST_OF_CLUSTER, restprivateIPs);
        allPrivateIPs.addAll(restprivateIPs);

        List<String> restpublicIPs = Results.flatMap(result -> result.map((row, rowMetadata) -> {
            String host = row.get("public_ip", String.class);
            String cloud = row.get("cloud", String.class);
            String region = row.get("region", String.class);
            String zone = row.get("zone", String.class);
            if (!allPublicIPs.contains(host)){
                return host;
            }
            return "";
        }))
                .collectList()
                .block();

        restpublicIPs.removeAll(Arrays.asList("", null));
        fallbackPublicIPs.put(REST_OF_CLUSTER, restpublicIPs);
        allPublicIPs.addAll(restpublicIPs);
        LOGGER.trace("Rest-of-cluster IPs - private: {}, public: {}", restprivateIPs, restpublicIPs);

        String hostConnectedTo = controlConnection.getResources().getConfiguration().getHostConnectedTo();
        List<String> hostsavailable = this.configuration.getHosts();
        if (allPrivateIPs.contains(hostConnectedTo)){
            LOGGER.debug("Using private hosts for connection");
            useHostColumn = Boolean.TRUE;
            for (String privateIP : allPrivateIPs) {
                if (!hostsavailable.contains(privateIP)) {
                    this.configuration.setHosts(privateIP);
                }
            }
        }
        else if (allPublicIPs.contains(hostConnectedTo)) {
            LOGGER.debug("Using public IPs for connection");
            useHostColumn = Boolean.FALSE;
            for (String publicIP : allPublicIPs) {
                if (!hostsavailable.contains(publicIP)) {
                    this.configuration.setHosts(publicIP);
                }
            }
        }

        LOGGER.debug("Current addresses: Private: {}, Public: {}", privateHosts, currentPublicIps);
        return getPrivateOrPublicServers(privateHosts, currentPublicIps);
    }

    @Override
    protected List<String> getPrivateOrPublicServers(List<String> privateHosts,
                                                          List<String> publicHosts) {
        List<String> servers = super.getPrivateOrPublicServers(privateHosts, publicHosts);
        if (servers != null && !servers.isEmpty()) {
            LOGGER.trace("Returning {} servers from primary placements: {}", servers.size(), servers);
            return servers;
        }
        LOGGER.debug("No servers available in primary placements, exploring servers in fallback placements.");
        for (int i = FIRST_FALLBACK; i <= MAX_PREFERENCE_VALUE; i++) {
            if (fallbackPrivateIPs.get(i) != null && !fallbackPrivateIPs.get(i).isEmpty()) {
                LOGGER.debug("Found servers at fallback preference level {}: private={}, public={}", i, fallbackPrivateIPs.get(i), fallbackPublicIPs.get(i));
                return super.getPrivateOrPublicServers(fallbackPrivateIPs.get(i), fallbackPublicIPs.get(i));
            }
        }
        // If no servers are available in fallback placements then attempt rest of the cluster.
        LOGGER.debug("No servers available in fallback placements, exploring rest of the cluster.");
        return super.getPrivateOrPublicServers(fallbackPrivateIPs.get(REST_OF_CLUSTER),
                fallbackPublicIPs.get(REST_OF_CLUSTER));
    }

    @Override
    public synchronized boolean hasMorePreferredNode(String chosenHost) {
        if (hostToPriorityMap.containsKey(chosenHost)) {
            Integer chosenHostPriority = hostToPriorityMap.get(chosenHost);
            if (chosenHostPriority != null) {
                for (int i = 1; i < chosenHostPriority; i++) {
                    if (hostToPriorityMap.values().contains(i)) {
                        LOGGER.debug("Host {} has priority {}, but a node with priority {} exists. Preferring better node", chosenHost, chosenHostPriority, i);
                        hostToNumConnCountMap.put(chosenHost, hostToNumConnMap.get(chosenHost));
                        return true;
                    }
                }
            }
        }
        LOGGER.trace("No better preferred node than {} (priority: {})", chosenHost, hostToPriorityMap.get(chosenHost));
        return false;
    }

    @Override
    protected void updatePriorityMap(String host, String cloud, String region, String zone) {
        if (!unreachableHosts.containsKey(host)) {
            int priority = getPriority(cloud, region, zone);
            hostToPriorityMap.put(host, priority);
            LOGGER.trace("Host {} at {}.{}.{} assigned priority {}", host, cloud, region, zone, priority);
        } else {
            LOGGER.trace("Skipping priority update for unreachable host {}", host);
        }
    }

    private int getPriority(String cloud, String region, String zone) {
        CloudPlacement cp = new CloudPlacement(cloud, region, zone);
        return getKeysByValue(cp);
    }

    private int getKeysByValue(CloudPlacement cp) {
        int i;
        for (i = 1; i <= MAX_PREFERENCE_VALUE; i++) {
            if (allowedPlacements.get(i) != null && !allowedPlacements.get(i).isEmpty()) {
                if (cp.isContainedIn(allowedPlacements.get(i))) {
                    return i;
                }
            }
        }
        return MAX_PREFERENCE_VALUE + 1;
    }

    @Override
    public synchronized void updateFailedHosts(String chosenHost) {
        super.updateFailedHosts(chosenHost);
        for (int i = FIRST_FALLBACK; i <= MAX_PREFERENCE_VALUE; i++) {
            if (fallbackPrivateIPs.get(i) != null && !fallbackPrivateIPs.get(i).isEmpty()) {
                if (fallbackPrivateIPs.get(i).contains(chosenHost)) {
                    List<String> hosts = fallbackPrivateIPs.computeIfAbsent(i, k -> new ArrayList<>());
                    hosts.remove(chosenHost);
                    LOGGER.trace("Removed {} from fallback private IPs at preference level {}", chosenHost, i);
                    return;
                }
            }
            if (fallbackPublicIPs.get(i) != null && !fallbackPublicIPs.get(i).isEmpty()) {
                if (fallbackPublicIPs.get(i).contains(chosenHost)) {
                    List<String> hosts = fallbackPublicIPs.computeIfAbsent(i, k -> new ArrayList<>());
                    hosts.remove(chosenHost);
                    LOGGER.trace("Removed {} from fallback public IPs at preference level {}", chosenHost, i);
                    return;
                }
            }
        }
        if (fallbackPrivateIPs.get(REST_OF_CLUSTER) != null) {
            if (fallbackPrivateIPs.get(REST_OF_CLUSTER).contains(chosenHost)) {
                List<String> hosts = fallbackPrivateIPs.computeIfAbsent(REST_OF_CLUSTER,
                        k -> new ArrayList<>());
                hosts.remove(chosenHost);
                LOGGER.trace("Removed {} from rest-of-cluster private IPs", chosenHost);
                return;
            }
        }

        if (fallbackPublicIPs.get(REST_OF_CLUSTER) != null) {
            if (fallbackPublicIPs.get(REST_OF_CLUSTER).contains(chosenHost)) {
                List<String> hosts = fallbackPublicIPs.computeIfAbsent(REST_OF_CLUSTER,
                        k -> new ArrayList<>());
                hosts.remove(chosenHost);
                LOGGER.trace("Removed {} from rest-of-cluster public IPs", chosenHost);
            }
        }
    }

    @Override
    public Mono<Client> connect() {
        return null;
    }

    public Mono<Client> connect(String host) {
        incDecConnectionCount(host, 1);
        endpoint = InetSocketAddress.createUnresolved(host, 5433);
        Mono<Client> client = this.connectionFunction.connect(endpoint, this.connectionSettings);
        return client;
    }

    static class CloudPlacement {
        private final String cloud;
        private final String region;
        private final String zone;

        CloudPlacement(String cloud, String region, String zone) {
            this.cloud = cloud;
            this.region = region;
            this.zone = zone;
        }

        public boolean isContainedIn(Set<CloudPlacement> set) {
            if (this.zone.equals("*")) {
                for (CloudPlacement cp : set) {
                    if (cp.cloud.equalsIgnoreCase(this.cloud) && cp.region.equalsIgnoreCase(this.region)) {
                        return true;
                    }
                }
            } else {
                for (CloudPlacement cp : set) {
                    if (cp.cloud.equalsIgnoreCase(this.cloud)
                            && cp.region.equalsIgnoreCase(this.region)
                            && (cp.zone.equalsIgnoreCase(this.zone) || cp.zone.equals("*"))) {
                        return true;
                    }
                }
            }
            return false;
        }

        public int hashCode() {
            return cloud.hashCode() ^ region.hashCode() ^ zone.hashCode();
        }

        public boolean equals(Object other) {
            boolean equal = false;
            if (other instanceof CloudPlacement) {
                CloudPlacement o = (CloudPlacement) other;
                equal = this.cloud.equalsIgnoreCase(o.cloud) &&
                        this.region.equalsIgnoreCase(o.region) &&
                        this.zone.equalsIgnoreCase(o.zone);
            }
            return equal;
        }

        public String toString() {
            return "Placement: " + cloud + "." + region + "." + zone;
        }
    }
}
