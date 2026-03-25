package com.yugabyte;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;

import java.time.Duration;

import reactor.core.publisher.Mono;

public class BasicPoolTest {

  public static void main(String[] args) {
    // Expects a YB rf=3 cluster running locally
    PostgresqlConnectionFactory driverFactory = new PostgresqlConnectionFactory(
            PostgresqlConnectionConfiguration.builder()
                    .host("127.0.0.1")
                    .host("127.0.0.2")
                    .port(5433)
                    .database("yugabyte")
                    .username("yugabyte")
                    .password("yugabyte")
                    .loadBalanceHosts(true)
                    .build()
    );

    ConnectionPoolConfiguration config = ConnectionPoolConfiguration.builder(driverFactory)
            .initialSize(3)
            .maxSize(3)
            .maxIdleTime(Duration.ofMinutes(30))
            .build();

    ConnectionPool pool = new ConnectionPool(config);

    try {
      System.out.println("Sleeping before running queries");
      Thread.sleep(5000);
      System.out.println("DONE - Sleeping before running queries");
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    Mono<String> op1 = runSlowQuery(pool, 1);
    Mono<String> op2 = runSlowQuery(pool, 2);
    Mono<String> op3 = runSlowQuery(pool, 3);

    System.out.println("Launching 3 concurrent operations on a pool of size 3...");

    Mono.zip(op1, op2, op3)
            .doOnSuccess(tuple -> {
              System.out.println("All 3 operations completed:");
              System.out.println("  Op1: " + tuple.getT1());
              System.out.println("  Op2: " + tuple.getT2());
              System.out.println("  Op3: " + tuple.getT3());
            })
            .block();

    System.out.println(">>>>>>>    Now manually check the count on all 3 rpcz endpoints");

    pool.dispose();
    System.out.println("Pool disposed. Done.");
  }

  private static Mono<String> runSlowQuery(ConnectionPool pool, int opId) {
    return Mono.from(pool.create())
            .flatMap(connection -> {
              System.out.println("Op" + opId + ": acquired connection");
              return Mono.from(connection.createStatement("SELECT pg_sleep(5), " + opId + " AS op_id").execute())
                      .flatMap(result -> Mono.from(result.map((row, meta) -> "op_id=" + row.get("op_id", Integer.class))))
                      .doFinally(sig -> {
                        System.out.println("Op" + opId + ": releasing connection");
                        Mono.from(connection.close()).subscribe();
                      });
            });
  }
}
