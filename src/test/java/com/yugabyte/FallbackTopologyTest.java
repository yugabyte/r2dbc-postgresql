package com.yugabyte;

import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.postgresql.UniformLoadBalancerConnectionStrategy;
import io.r2dbc.postgresql.api.PostgresqlConnection;

import java.util.ArrayList;

public class FallbackTopologyTest {
    public static void main (String args[]){

        PostgresqlConnectionFactory connectionFactory = new PostgresqlConnectionFactory(PostgresqlConnectionConfiguration.builder()
                .addHost("127.0.0.3",5433)
                .username("yugabyte")
                .password("yugabyte")
                .database("yugabyte")
                .loadBalanceHosts(true)
                .topologyKeys("cloud1.datacenter1.rack1:1,cloud2.datacenter2.rack2:2")
                .build());

        ArrayList<PostgresqlConnection> connections = new ArrayList<>();

        for (int i = 0; i < 12; i++) {
            PostgresqlConnection connection = connectionFactory.create().block();
            connections.add(connection);
        }

        UniformLoadBalancerConnectionStrategy strategy = new UniformLoadBalancerConnectionStrategy();
        strategy.printCurrentConnectionCounts();

        for (PostgresqlConnection connection: connections) {
            connection.close().block();
        }

    }
}
