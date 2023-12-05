package io.r2dbc.postgresql;

import io.r2dbc.postgresql.api.PostgresqlResult;
import io.r2dbc.postgresql.client.Client;
import io.r2dbc.postgresql.client.ConnectionSettings;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

public class UniformLoadBalancerConnectionStrategy implements ConnectionStrategy {

    private final ConnectionFunction connectionFunction;

    private final PostgresqlConnectionConfiguration configuration;

    private final ConnectionSettings connectionSettings;

    protected static List<String> servers = new ArrayList<>();

    ConcurrentHashMap<String, Integer> hostToNumConnMap = new ConcurrentHashMap<>();
    ConcurrentHashMap<String, Integer> hostToNumConnCount = new ConcurrentHashMap<>();
    final ConcurrentHashMap<String, Integer> hostToPriorityMap = new ConcurrentHashMap<>();

    protected SocketAddress endpoint;

    private long lastServerListFetchTime = 0L;
    protected int refreshListSeconds = 300;
    List<String> unreachableHosts = new ArrayList<>();
    protected Boolean useHostColumn = null;
    protected List<String> currentPublicIps = new ArrayList<>();

    public UniformLoadBalancerConnectionStrategy(){
        this.connectionFunction = null;
        this.configuration = null;
        this.connectionSettings = null;
    }

    UniformLoadBalancerConnectionStrategy(ConnectionFunction connectionFunction, PostgresqlConnectionConfiguration configuration, ConnectionSettings settings, int refreshListSeconds) {

        this.connectionFunction = connectionFunction;
        this.configuration = configuration;
        this.connectionSettings = settings;
        this.refreshListSeconds = refreshListSeconds > 0 && refreshListSeconds <= 600 ?
                refreshListSeconds : 300;
    }

    public void printCurrentConnectionCounts() {
        for (String host : hostToNumConnMap.keySet()) {
            System.out.println("Host: " + host + " has " + hostToNumConnMap.get(host) + " connections");
        }
    }

    // Create a ConcurrentHashMap to store the addresses and their respective
    // connection counts
    // by querying "Select * from yb_servers()" using the control connection
    protected List<String> getCurrentServers(PostgresqlConnection controlConnection) {
        currentPublicIps.clear();
        Flux<PostgresqlResult> Results = controlConnection.createStatement("Select * from yb_servers()").execute();
        List<String> privateHosts = Results.flatMap(result -> result.map((row, rowMetaData) -> row.get("host", String.class)))
                 .collectList().block();
        currentPublicIps = Results.flatMap(result -> result.map((row, rowMetaData) -> row.get("public_ip", String.class)))
                .collectList().block();
        String hostConnectedTo = controlConnection.getResources().getConfiguration().getHostConnectedTo();
        List<String> hostsavailable = this.configuration.getHosts();
            if (privateHosts.contains(hostConnectedTo)){
                useHostColumn = Boolean.TRUE;
                for(String host : privateHosts) {
                    if (!hostsavailable.contains(host)) {
                        this.configuration.setHosts(host);
                    }
                }
            }
            else if (currentPublicIps.contains(hostConnectedTo)) {
                useHostColumn = Boolean.FALSE;
                for(String host : currentPublicIps) {
                    if (!hostsavailable.contains(host)) {
                        this.configuration.setHosts(host);
                    }
                }
            }
        return getPrivateOrPublicServers(privateHosts, currentPublicIps);
    }


    protected List<String> getPrivateOrPublicServers(List<String> privateHosts,
                                                          List<String> publicHosts) {
        if (useHostColumn == null) {
            if (publicHosts.isEmpty()) {
                useHostColumn = Boolean.TRUE;
            }
            return privateHosts;
        }
        List<String> currentHosts = useHostColumn ? privateHosts : publicHosts;
        return currentHosts;
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

    public synchronized void updateFailedHosts(String chosenHost) {
        unreachableHosts.add(chosenHost);
        hostToNumConnMap.remove(chosenHost);
    }

    public boolean needsRefresh() {
        long currentTimeInMillis = System.currentTimeMillis();
        long diff = (currentTimeInMillis - lastServerListFetchTime) / 1000;
        boolean firstTime = servers == null;
        if (firstTime || diff > refreshListSeconds) {
            return true;
        }
        return false;
    }

    public synchronized boolean refresh(PostgresqlConnection controlConnection) {
        if (!needsRefresh()) {
            return true;
        }
        // else clear server list
        long currTime = System.currentTimeMillis();
        lastServerListFetchTime = currTime;

        servers = getCurrentServers(controlConnection);
        unreachableHosts.clear();
        if (servers == null) {
            return false;
        }

        for (String h : servers) {
            if (!hostToNumConnMap.containsKey(h)) {
                hostToNumConnMap.put(h, 0);
            }
        }
        return true;
    }

    public void setForRefresh() {
        lastServerListFetchTime = 0L;
    }

    // When a connection is closed, decrement the connection count for the host

    public void incDecConnectionCount(String host, int incDec) {
        if (hostToNumConnMap.get(host) == null)
            return;
        if (hostToNumConnMap.get(host) == 0 && incDec < 0)
            return;
        hostToNumConnMap.put(host, hostToNumConnMap.get(host) + incDec);
    }

    // Get the host with the least number of connections
    public String getHostWithLeastConnections() {
        if(hostToNumConnMap.isEmpty()){
            servers = getPrivateOrPublicServers(new ArrayList<>(), currentPublicIps);
            if (servers != null && !servers.isEmpty()) {
                for (String h : servers) {
                    if (!hostToNumConnMap.containsKey(h)) {
                        hostToNumConnMap.put(h, 0);
                    }
                }
            } else {
                return null;
            }
        }
        String hostWithLeastConnections = null;
        int leastConnections = Integer.MAX_VALUE;
        ArrayList<String> minConnectionsHostList = new ArrayList<>();
        for (String host : hostToNumConnMap.keySet()) {
            int currLoad = hostToNumConnMap.get(host);
            if (!unreachableHosts.contains(host) && currLoad < leastConnections) {
                leastConnections = currLoad;
                minConnectionsHostList.clear();
                minConnectionsHostList.add(host);
            }
            else if (!unreachableHosts.contains(host) && currLoad == leastConnections) {
                minConnectionsHostList.add(host);
            }
        }
        if (minConnectionsHostList.size() > 0) {
            hostWithLeastConnections = minConnectionsHostList.get(new Random().nextInt(minConnectionsHostList.size()));
        }

        return hostWithLeastConnections;
    }

    public boolean hasMorePreferredNode(String chosenHost) {
        return false;
    }

    protected void updatePriorityMap(String host, String cloud, String region, String zone) {
    }
}

