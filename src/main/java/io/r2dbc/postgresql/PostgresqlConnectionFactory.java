/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.r2dbc.postgresql;

import io.netty.buffer.ByteBufAllocator;
import io.r2dbc.postgresql.api.ErrorDetails;
import io.r2dbc.postgresql.api.PostgresqlException;
import io.r2dbc.postgresql.client.Client;
import io.r2dbc.postgresql.client.ConnectionSettings;
import io.r2dbc.postgresql.client.ReactorNettyClient;
import io.r2dbc.postgresql.codec.DefaultCodecs;
import io.r2dbc.postgresql.extension.CodecRegistrar;
import io.r2dbc.postgresql.util.Assert;
import io.r2dbc.postgresql.util.Operators;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.annotation.Nullable;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.time.ZoneId;
import java.util.*;

/**
 * An implementation of {@link ConnectionFactory} for creating connections to a PostgreSQL database.
 */
public final class PostgresqlConnectionFactory implements ConnectionFactory {

    private static final ConnectionFunction DEFAULT_CONNECTION_FUNCTION = (endpoint, settings) ->
        ReactorNettyClient.connect(endpoint, settings).cast(Client.class);

    private static final String REPLICATION_OPTION = "replication";

    private static final String REPLICATION_DATABASE = "database";

    private final ConnectionFunction connectionFunction;

    private final PostgresqlConnectionConfiguration configuration;

    private final Extensions extensions;

    // YugabyteDB specific

    public static PostgresqlConnection controlConnection = null;

    private static Map<String, UniformLoadBalancerConnectionStrategy> connectionStrategyMap = new LinkedHashMap<>();
    private static final Logger LOGGER = Loggers.getLogger(PostgresqlConnectionFactory.class.getName());

    /**
     * Create a new connection factory.
     *
     * @param configuration the configuration to use
     * @throws IllegalArgumentException if {@code configuration} is {@code null}
     */
    public PostgresqlConnectionFactory(PostgresqlConnectionConfiguration configuration) {
        this(DEFAULT_CONNECTION_FUNCTION, configuration);
    }

    /**
     * Create a new connection factory.
     *
     * @param connectionFunction the connectionFunction to establish
     * @param configuration      the configuration to use
     * @throws IllegalArgumentException if {@code configuration} is {@code null}
     */
    PostgresqlConnectionFactory(ConnectionFunction connectionFunction, PostgresqlConnectionConfiguration configuration) {
        this.connectionFunction = Assert.requireNonNull(connectionFunction, "connectionFunction must not be null");
        this.configuration = Assert.requireNonNull(configuration, "configuration must not be null");
        this.extensions = getExtensions(configuration);
    }

    private static Extensions getExtensions(PostgresqlConnectionConfiguration configuration) {
        Extensions extensions = Extensions.from(configuration.getExtensions());

        if (configuration.isAutodetectExtensions()) {
            extensions = extensions.mergeWith(Extensions.autodetect());
        }

        return extensions;
    }

    @Override
    public Mono<io.r2dbc.postgresql.api.PostgresqlConnection> create() {

        // Mono.defer: r2dbc-pool calls factory.create() once and resubscribes to the
        // returned Mono for each pool slot; defer triggers fresh host selection per subscription.
        // subscribeOn(boundedElastic): r2dbc-pool runs the allocator on Schedulers.single()
        // which forbids block(); our load-balanced path needs block() for control-connection
        // setup and yb_servers() refresh, so we switch to a blocking-capable scheduler.
        return Mono.defer(() -> {
            if (isReplicationConnection()) {
                throw new UnsupportedOperationException("Cannot create replication connection through create(). Use replication() method instead.");
            }

            if (this.configuration.isLoadBalanced()) {
                 LOGGER.trace("Load balancing is enabled. Trying to create a load balanced connection");
                Mono<io.r2dbc.postgresql.api.PostgresqlConnection> conn = createLoadBalancedConnection();
                if (conn != null) {
                    return conn;
                }
                LOGGER.warn("Failed to apply load balance. Trying normal connection");
            }
            ConnectionStrategy connectionStrategy = ConnectionStrategyFactory.getConnectionStrategy(this.connectionFunction, this.configuration, this.configuration.getConnectionSettings());
            return doCreateConnection(false, connectionStrategy).cast(io.r2dbc.postgresql.api.PostgresqlConnection.class);
        }).subscribeOn(reactor.core.scheduler.Schedulers.boundedElastic());
    }

    private synchronized boolean createControlConnection() {
        List<String> hosts = this.configuration.getHosts();
        LOGGER.debug("Attempting to create control connection. Candidate hosts: {}", hosts);
        UniformLoadBalancerConnectionStrategy strategy = getAppropriateLoadBalancer();
        for (Iterator<String> iterator = hosts.iterator(); iterator.hasNext(); ) {
            String host = iterator.next();
            ConnectionFunction connectionFunction = new SingleHostConnectionFunction(this.connectionFunction, this.configuration);
            try {
                LOGGER.trace("Trying control connection to host: {}", host);
                controlConnection = doCreateConnection(strategy, false, connectionFunction, host, true).block();
                if (controlConnection != null) {
                    LOGGER.info("Control connection established to host: {}", host);
                    return true;
                }
                LOGGER.trace("Control connection to host {} returned null", host);
            } catch (Exception e) {
                LOGGER.debug("Control connection to host {} failed: {}", host, e.getMessage());
                iterator.remove();
            }
        }
        LOGGER.warn("Failed to create control connection. All candidate hosts exhausted");
        return false;
    }

    private synchronized Mono<io.r2dbc.postgresql.api.PostgresqlConnection>  createLoadBalancedConnection() {
        LOGGER.trace("Entering createLoadBalancedConnection()");
        PostgresqlConnection newConn = null;
        String chosenHost = null;
        UniformLoadBalancerConnectionStrategy connectionStrategy = getAppropriateLoadBalancer();
        List<String> hosts = this.configuration.getHosts();
            if (chosenHost == null && controlConnection == null) {
                LOGGER.debug("No existing control connection. Establishing one from seed hosts: {}", hosts);
                for (Iterator<String> iterator = hosts.iterator(); iterator.hasNext();) {
                    String host = iterator.next();
                    ConnectionFunction connectionFunction = new SingleHostConnectionFunction(this.connectionFunction, this.configuration);
                    try{
                        LOGGER.trace("Trying control connection to seed host: {}", host);
                        controlConnection = doCreateConnection(connectionStrategy,false, connectionFunction, host, true).block();
                        if (controlConnection != null) {
                            LOGGER.info("Control connection established to host: {}", host);
                            break;
                        }
                    }catch (Exception ex){
                        LOGGER.debug("Control connection to seed host {} failed: {}. Removing from candidate list", host, ex.getMessage());
                        iterator.remove();
                        if (hosts.isEmpty()) {
                            LOGGER.warn("No hosts available for control connection.");
                            return null;
                        }
                    }
                }
            }

        while (true) {
            try {
                if  (controlConnection == null || !connectionStrategy.refresh(controlConnection)) {
                    LOGGER.warn("Server list refresh failed. controlConnection={}", controlConnection != null ? "present" : "null");
                    return null;
                } else {
                    LOGGER.trace("Server list refresh succeeded");
                    break;
                }
            } catch (R2dbcNonTransientResourceException e) {
                String failedHost = controlConnection.getResources().getConfiguration().getHostConnectedTo();
                LOGGER.warn("Control connection to {} lost ({}). Attempting to re-establish", failedHost, e.getMessage());
                connectionStrategy.updateFailedHosts(failedHost);
                boolean success = createControlConnection();
                if (success) {
                    break;
                } else {
                    LOGGER.warn("Failed to re-establish control connection. Aborting attempt to get a load balanced connection");
                    return null;
                }
            }
        }

        chosenHost = connectionStrategy.getHostWithLeastConnections();

        if (chosenHost == null) {
            LOGGER.warn("No available host found with least connections. Returning null");
            return null;
        }

        LOGGER.debug("Selected host with least connections: {}", chosenHost);

        Mono<PostgresqlConnection> newConnection = null;
        while(chosenHost != null){
            try {
                LOGGER.trace("Attempting data connection to host: {}", chosenHost);
                newConnection = doCreateConnection(connectionStrategy,false, null, chosenHost, false);
                if (newConnection == null || !connectionStrategy.refresh(newConnection)) {
                    if (newConnection == null) {
                        LOGGER.debug("Data connection to {} failed", chosenHost);
                    } else {
                        LOGGER.debug("refresh() returned false");
                    }
                    connectionStrategy.incDecConnectionCount(chosenHost, -1);
                    connectionStrategy.updateFailedHosts(chosenHost);
                    connectionStrategy.setForRefresh();
                }
                else {
                    boolean betterNodeAvailable = connectionStrategy.hasMorePreferredNode(chosenHost);
                    if (betterNodeAvailable){
                        LOGGER.debug("A better node than {} is available. Will attempt a connection to it", chosenHost);
                        connectionStrategy.incDecConnectionCount(chosenHost, -1);
                        return createLoadBalancedConnection();
                    }
                    LOGGER.trace("Data connection to {} established successfully", chosenHost);
                    return newConnection.cast(io.r2dbc.postgresql.api.PostgresqlConnection.class);
                }
            }catch (Exception ex){
                LOGGER.info("Exception during data connection to {}: {}. Cleaning up and retrying", chosenHost, ex.getMessage());
                connectionStrategy.setForRefresh();
                try {
                    newConnection.block().close().block();
                    newConn.close().block();
                }catch (Exception e) {
                    LOGGER.trace("Cleanup of failed connection threw: {}", e.getMessage());
                }
                connectionStrategy.updateFailedHosts(chosenHost);
            }
            chosenHost = connectionStrategy.getHostWithLeastConnections();
            if (chosenHost != null) {
                LOGGER.debug("Retrying with next least-loaded host: {}", chosenHost);
            }
        }
        LOGGER.warn("All hosts exhausted during load balanced connection creation. Returning null");
        return null;
    }

    private UniformLoadBalancerConnectionStrategy getAppropriateLoadBalancer(){
        UniformLoadBalancerConnectionStrategy connectionStrategy;
        if (this.configuration.getTopologyKeys() != null) {
            synchronized (connectionStrategyMap) {
                connectionStrategy = connectionStrategyMap.get(this.configuration.getTopologyKeys());
                if (connectionStrategy == null) {
                    LOGGER.debug("Creating new TopologyAwareLoadBalancerConnectionStrategy for topology-keys: {}", this.configuration.getTopologyKeys());
                    connectionStrategy = new TopologyAwareLoadBalancerConnectionStrategy(new SingleHostConnectionFunction(this.connectionFunction, this.configuration), this.configuration, this.configuration.getTopologyKeys(), this.configuration.getConnectionSettings(), this.configuration.getYBServersRefreshInterval());
                    connectionStrategyMap.put(this.configuration.getTopologyKeys(), connectionStrategy);
                } else {
                    LOGGER.trace("Reusing existing TopologyAwareLoadBalancerConnectionStrategy for topology-keys: {}", this.configuration.getTopologyKeys());
                }
            }
        }
        else{
            synchronized (connectionStrategyMap){
                connectionStrategy = connectionStrategyMap.get("UniformLoadBalancerConnectionStrategy");
                if (connectionStrategy == null) {
                    LOGGER.debug("Creating new UniformLoadBalancerConnectionStrategy with refresh interval: {}s", this.configuration.getYBServersRefreshInterval());
                    connectionStrategy = new UniformLoadBalancerConnectionStrategy(new SingleHostConnectionFunction(this.connectionFunction, this.configuration), this.configuration, this.configuration.getConnectionSettings(), this.configuration.getYBServersRefreshInterval());
                    connectionStrategyMap.put("UniformLoadBalancerConnectionStrategy", connectionStrategy);
                } else {
                    LOGGER.trace("Reusing existing UniformLoadBalancerConnectionStrategy");
                }
            }
        }
        return connectionStrategy;
    }

    /**
     * Create a new {@link io.r2dbc.postgresql.api.PostgresqlReplicationConnection} for interaction with replication streams.
     *
     * @return a new {@link io.r2dbc.postgresql.api.PostgresqlReplicationConnection} for interaction with replication streams.
     */
    public Mono<io.r2dbc.postgresql.api.PostgresqlReplicationConnection> replication() {

        Map<String, String> options = new LinkedHashMap<>(this.configuration.getOptions());
        options.put(REPLICATION_OPTION, REPLICATION_DATABASE);

        ConnectionSettings connectionSettings = this.configuration.getConnectionSettings().mutate(builder -> builder.startupOptions(options));

        ConnectionStrategy connectionStrategy = ConnectionStrategyFactory.getConnectionStrategy(this.connectionFunction, this.configuration, connectionSettings);

        return doCreateConnection(true, connectionStrategy).map(DefaultPostgresqlReplicationConnection::new);
    }

    private Mono<PostgresqlConnection> doCreateConnection(UniformLoadBalancerConnectionStrategy connectionStrategy, boolean forReplication, ConnectionFunction connectionFunction, String host, boolean isControlConnection) {

        LOGGER.trace("doCreateConnection: host={}, isControlConnection={}", host, isControlConnection);
        ZoneId defaultZone = TimeZone.getDefault().toZoneId();
        SocketAddress endpoint = InetSocketAddress.createUnresolved(host, 5433);

        PostgresqlConnectionConfiguration newConfig = new PostgresqlConnectionConfiguration(this.configuration);
        newConfig.setHostConnectedTo(host);

        Mono<Client> connclient = isControlConnection ? connectionFunction.connect(endpoint, newConfig.getConnectionSettings()) : connectionStrategy.connect(host);

        return connclient
                .flatMap(client -> {

                    DefaultCodecs codecs = new DefaultCodecs(client.getByteBufAllocator(), newConfig.isPreferAttachedBuffers(),
                            () -> client.getTimeZone().map(TimeZone::toZoneId).orElse(defaultZone));
                    StatementCache statementCache = StatementCache.fromPreparedStatementCacheQueries(client, newConfig.getPreparedStatementCacheQueries());

                    // early connection object to retrieve initialization details
                    PostgresqlConnection earlyConnection = new PostgresqlConnection(client, codecs, DefaultPortalNameSupplier.INSTANCE, statementCache, IsolationLevel.READ_COMMITTED,
                            newConfig);

                    Mono<IsolationLevel> isolationLevelMono = Mono.just(IsolationLevel.READ_COMMITTED);
                    if (!forReplication) {
                        isolationLevelMono = getIsolationLevel(earlyConnection);
                    }
                    return isolationLevelMono
                            // actual connection to be used
                            .map(isolationLevel -> {
                                PostgresqlConnection conn = new PostgresqlConnection(client, codecs, DefaultPortalNameSupplier.INSTANCE, statementCache, isolationLevel, newConfig);
                                conn.setConnectionStrategy(connectionStrategy);
                                return conn;
                            })
                            .delayUntil(connection -> {
                                return prepareConnection(connection, client.getByteBufAllocator(), codecs, forReplication);
                            });
                })
                .onErrorResume(throwable -> {
                    if(!isControlConnection) {
                        LOGGER.info("{} not reachable ({}), adding to failed list and retrying", host, throwable.getMessage());
                        connectionStrategy.incDecConnectionCount(host, -1);
                        connectionStrategy.updateFailedHosts(host);
                        Mono<io.r2dbc.postgresql.api.PostgresqlConnection> connectionMono = createLoadBalancedConnection();
                        return connectionMono == null ? null : connectionMono.cast(PostgresqlConnection.class);
                    } else {
                        LOGGER.warn("Control connection to {} failed: {}", host, throwable.getMessage());
                        return null;
                    }
                    // todo setForceRefresh() needed like in pgjdbc?
                })
                .flux()
                .as(Operators::discardOnCancel)
                .single()
                .doOnDiscard(PostgresqlConnection.class, client -> client.close().subscribe());
    }

    private Mono<PostgresqlConnection> doCreateConnection(boolean forReplication, ConnectionStrategy connectionStrategy) {

        ZoneId defaultZone = TimeZone.getDefault().toZoneId();

        return connectionStrategy.connect()
            .flatMap(client -> {

                DefaultCodecs codecs = new DefaultCodecs(client.getByteBufAllocator(), this.configuration.isPreferAttachedBuffers(),
                    () -> client.getTimeZone().map(TimeZone::toZoneId).orElse(defaultZone));
                StatementCache statementCache = StatementCache.fromPreparedStatementCacheQueries(client, this.configuration.getPreparedStatementCacheQueries());

                // early connection object to retrieve initialization details
                PostgresqlConnection earlyConnection = new PostgresqlConnection(client, codecs, DefaultPortalNameSupplier.INSTANCE, statementCache, IsolationLevel.READ_COMMITTED,
                    this.configuration);

                Mono<IsolationLevel> isolationLevelMono = Mono.just(IsolationLevel.READ_COMMITTED);
                if (!forReplication) {
                    isolationLevelMono = getIsolationLevel(earlyConnection);
                }
                return isolationLevelMono
                    // actual connection to be used
                    .map(isolationLevel -> new PostgresqlConnection(client, codecs, DefaultPortalNameSupplier.INSTANCE, statementCache, isolationLevel, this.configuration))
                    .delayUntil(connection -> {
                        return prepareConnection(connection, client.getByteBufAllocator(), codecs, forReplication);
                    })
                    .onErrorResume(throwable -> this.closeWithError(client, throwable));
            }).onErrorMap(e -> cannotConnect(e, connectionStrategy))
            .flux()
            .as(Operators::discardOnCancel)
            .single()
            .doOnDiscard(PostgresqlConnection.class, client -> client.close().subscribe());
    }

    private boolean isReplicationConnection() {
        Map<String, String> options = this.configuration.getOptions();
        return REPLICATION_DATABASE.equalsIgnoreCase(options.get(REPLICATION_OPTION));
    }

    private Publisher<?> prepareConnection(PostgresqlConnection connection, ByteBufAllocator byteBufAllocator, DefaultCodecs codecs, boolean forReplication) {

        List<Publisher<?>> publishers = new ArrayList<>();

        if (!forReplication) {
            this.extensions.forEach(CodecRegistrar.class, it -> {
                publishers.add(it.register(connection, byteBufAllocator, codecs));
            });
        }

        return Flux.concat(publishers).then();
    }

    private Mono<PostgresqlConnection> closeWithError(Client client, Throwable throwable) {
        return client.close().then(Mono.error(throwable));
    }

    private Throwable cannotConnect(Throwable throwable, ConnectionStrategy strategy) {

        if (throwable instanceof R2dbcException) {
            return throwable;
        }

        return new PostgresConnectionException(String.format("Cannot connect to %s", strategy), throwable);
    }

    @Override
    public ConnectionFactoryMetadata getMetadata() {
        return PostgresqlConnectionFactoryMetadata.INSTANCE;
    }

    PostgresqlConnectionConfiguration getConfiguration() {
        return this.configuration;
    }

    @Override
    public String toString() {
        return "PostgresqlConnectionFactory{" +
            ", configuration=" + this.configuration +
            ", extensions=" + this.extensions +
            '}';
    }

    private Mono<IsolationLevel> getIsolationLevel(io.r2dbc.postgresql.api.PostgresqlConnection connection) {
        return connection.createStatement("SHOW TRANSACTION ISOLATION LEVEL")
            .fetchSize(0)
            .execute()
            .flatMap(it -> it.map((row, rowMetadata) -> {
                String level = row.get(0, String.class);

                if (level == null) {
                    return IsolationLevel.READ_COMMITTED; // Best guess.
                }

                return IsolationLevel.valueOf(level.toUpperCase(Locale.US));
            })).defaultIfEmpty(IsolationLevel.READ_COMMITTED).last();
    }

    static class PostgresConnectionException extends R2dbcNonTransientResourceException implements PostgresqlException {

        private static final String CONNECTION_DOES_NOT_EXIST = "08003";

        private final ErrorDetails errorDetails;

        public PostgresConnectionException(String reason, @Nullable Throwable cause) {
            super(reason, CONNECTION_DOES_NOT_EXIST, 0, null, cause);
            this.errorDetails = ErrorDetails.fromCodeAndMessage(CONNECTION_DOES_NOT_EXIST, reason);
        }

        @Override
        public ErrorDetails getErrorDetails() {
            return this.errorDetails;
        }

    }

}
