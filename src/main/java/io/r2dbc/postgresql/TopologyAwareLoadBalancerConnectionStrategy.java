package io.r2dbc.postgresql;

import io.r2dbc.postgresql.api.PostgresqlResult;
import io.r2dbc.postgresql.client.Client;
import io.r2dbc.postgresql.client.ConnectionSettings;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

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

    public TopologyAwareLoadBalancerConnectionStrategy(ConnectionFunction connectionFunction, PostgresqlConnectionConfiguration configuration, String placementvalues, ConnectionSettings settings, int refreshListSeconds) {
        super(connectionFunction,configuration,settings, refreshListSeconds);
        placements = placementvalues;
        this.connectionFunction = connectionFunction;
        this.configuration = configuration;
        this.connectionSettings = settings;
        this.refreshListSeconds = refreshListSeconds > 0 && refreshListSeconds <= 600 ?
                refreshListSeconds : 300;
        parseGeoLocations();
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
    }

    @Override
    protected List<String> getCurrentServers(PostgresqlConnection controlConnection){

        currentPublicIps.clear();
        Flux<PostgresqlResult> Results = controlConnection.createStatement("Select * from yb_servers()").execute();
        List<String> privateHosts = Results.flatMap(result -> result.map((row, rowMetadata) -> {
                    String host = row.get("host", String.class);
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

        privateHosts.removeAll(Arrays.asList("", null));

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


        for (Map.Entry<Integer, Set<CloudPlacement>> allowedCPs : allowedPlacements.entrySet()) {
            List<String> privateIPs = Results.flatMap(result -> result.map((row, rowMetadata) -> {
                        String host = row.get("host", String.class);
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

            privateIPs.removeAll(Arrays.asList("", null));
            fallbackPrivateIPs.put(allowedCPs.getKey(), privateIPs);

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
            fallbackPublicIPs.put(allowedCPs.getKey(), privateIPs);
        }

        return getPrivateOrPublicServers(privateHosts, currentPublicIps);
    }

    @Override
    protected List<String> getPrivateOrPublicServers(List<String> privateHosts,
                                                          List<String> publicHosts) {
        List<String> servers = super.getPrivateOrPublicServers(privateHosts, publicHosts);
        if (servers != null && !servers.isEmpty()) {
            return servers;
        }
        // If no servers are available in primary placements then attempt fallback nodes.
        for (int i = FIRST_FALLBACK; i <= MAX_PREFERENCE_VALUE; i++) {
            if (fallbackPrivateIPs.get(i) != null && !fallbackPrivateIPs.get(i).isEmpty()) {
                return super.getPrivateOrPublicServers(fallbackPrivateIPs.get(i), fallbackPublicIPs.get(i));
            }
        }
        // If no servers are available in fallback placements then attempt rest of the cluster.

        return super.getPrivateOrPublicServers(fallbackPrivateIPs.get(REST_OF_CLUSTER),
                fallbackPublicIPs.get(REST_OF_CLUSTER));
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
