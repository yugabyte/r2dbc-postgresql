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

    private static PostgresqlConnection controlConnection = null;

    private static Map<String, UniformLoadBalancerConnectionStrategy> connectionStrategyMap = new LinkedHashMap<>();

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

        if (isReplicationConnection()) {
            throw new UnsupportedOperationException("Cannot create replication connection through create(). Use replication() method instead.");
        }

        if (this.configuration.isLoadBalanced()) {
            Mono<io.r2dbc.postgresql.api.PostgresqlConnection> conn = createLoadBalancedConnection();
            if (conn != null) {
                return conn;
            }
        }
        ConnectionStrategy connectionStrategy = ConnectionStrategyFactory.getConnectionStrategy(this.connectionFunction, this.configuration, this.configuration.getConnectionSettings());
        return doCreateConnection(false, connectionStrategy).cast(io.r2dbc.postgresql.api.PostgresqlConnection.class);


    }

    private Mono<io.r2dbc.postgresql.api.PostgresqlConnection> createLoadBalancedConnection() {
        PostgresqlConnection newConn = null;
        String chosenHost = null;
        UniformLoadBalancerConnectionStrategy connectionStrategy = getAppropriateLoadBlancer();
        List<String> hosts = this.configuration.getHosts();
            if (chosenHost == null) {
                for (Iterator<String> iterator = hosts.iterator(); iterator.hasNext();) {
                    String host = iterator.next();
                    ConnectionFunction connectionFunction = new SingleHostConnectionFunction(this.connectionFunction, this.configuration);
                    try{
                        controlConnection = doCreateConnection(null,false, connectionFunction, host).block();
                        if (controlConnection != null) {
                            break;
                        }
                    }catch (Exception ex){
                        iterator.remove();
                        if (hosts.isEmpty())
                            throw ex;
                    }
                }
            }

        if (controlConnection == null || !connectionStrategy.refresh(controlConnection)) {
            return null;
        }
        controlConnection.close().block();
        chosenHost = connectionStrategy.getHostWithLeastConnections();

        if (chosenHost == null)
            return null;

        while(chosenHost != null){
            try{
                Mono<io.r2dbc.postgresql.api.PostgresqlConnection> newConnection = doCreateConnection(connectionStrategy,false, null, chosenHost).cast(io.r2dbc.postgresql.api.PostgresqlConnection.class);
                newConn = (PostgresqlConnection) newConnection.block();
                connectionStrategy.incDecConnectionCount(chosenHost, 1);
                if (!connectionStrategy.refresh(newConn)){
                    connectionStrategy.incDecConnectionCount(chosenHost, -1);
                    connectionStrategy.updateFailedHosts(chosenHost);
                    connectionStrategy.setForRefresh();
                    try {
                        newConn.close().block();
                    } catch (Exception e) {
                        // ignore as exception is expected. This close is for any other cleanup
                        // which the driver side may be doing
                    }
                }
                else {
                    boolean betterNodeAvailable = connectionStrategy.hasMorePreferredNode(chosenHost);
                    if (betterNodeAvailable){
                        connectionStrategy.incDecConnectionCount(chosenHost, -1);
                        newConn.close().block();
                        newConnection.block().close().block();
                        return createLoadBalancedConnection();
                    }
                    newConn.close().block();
                    return newConnection;
                }
            }catch (Exception ex){
                connectionStrategy.setForRefresh();
                try {
                    newConn.close().block();
                }catch (Exception e) {
                    // ignore as the connection is already bad that's why we are here. Calling
                    // close so that client side cleanup can happen.
                }
                connectionStrategy.updateFailedHosts(chosenHost);
            }
            chosenHost = connectionStrategy.getHostWithLeastConnections();
        }
        return null;
    }

    private UniformLoadBalancerConnectionStrategy getAppropriateLoadBlancer(){
        UniformLoadBalancerConnectionStrategy connectionStrategy;
        if (this.configuration.getTopologyKeys() != null) {
            connectionStrategy = connectionStrategyMap.get(this.configuration.getTopologyKeys());
            if (connectionStrategy == null){
                connectionStrategy = new TopologyAwareLoadBalancerConnectionStrategy(new SingleHostConnectionFunction(this.connectionFunction, this.configuration), this.configuration, this.configuration.getTopologyKeys(), this.configuration.getConnectionSettings(), this.configuration.getYBServersRefreshInterval());
                connectionStrategyMap.put(this.configuration.getTopologyKeys(), connectionStrategy);
            }
        }
        else{
            connectionStrategy = connectionStrategyMap.get("UniformLoadBalancerConnectionStrategy");
            if (connectionStrategy == null) {
                connectionStrategy = new UniformLoadBalancerConnectionStrategy(new SingleHostConnectionFunction(this.connectionFunction, this.configuration), this.configuration, this.configuration.getConnectionSettings(), this.configuration.getYBServersRefreshInterval());
                connectionStrategyMap.put("UniformLoadBalancerConnectionStrategy", connectionStrategy);
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

    private Mono<PostgresqlConnection> doCreateConnection(UniformLoadBalancerConnectionStrategy connectionStrategy, boolean forReplication, ConnectionFunction connectionFunction, String host) {

        ZoneId defaultZone = TimeZone.getDefault().toZoneId();
        SocketAddress endpoint = InetSocketAddress.createUnresolved(host, 5433);

        PostgresqlConnectionConfiguration newConfig = this.configuration;
        newConfig.setHostConnectedTo(host);

        Mono<Client> connclient = connectionStrategy == null? connectionFunction.connect(endpoint, newConfig.getConnectionSettings()) : connectionStrategy.connect(host);

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
                            })
                            .onErrorResume(throwable -> this.closeWithError(client, throwable));
                }).onErrorMap(e -> {
                    if (e instanceof R2dbcException) {
                        return e;
                    }
                    return new R2dbcNonTransientResourceException(String.format("Cannot create control connection using %s", this.connectionFunction), e);
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
