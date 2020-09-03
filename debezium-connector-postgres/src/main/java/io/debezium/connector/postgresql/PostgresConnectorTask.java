/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import java.nio.charset.Charset;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.ReplicationConnection;
import io.debezium.connector.postgresql.spi.SlotCreationResult;
import io.debezium.connector.postgresql.spi.SlotState;
import io.debezium.connector.postgresql.spi.Snapshotter;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;
import io.debezium.util.Clock;
import io.debezium.util.LoggingContext;
import io.debezium.util.Metronome;

/**
 * Kafka connect source task which uses Postgres logical decoding over a streaming replication connection to process DB
 * changes.
 *
 * @author Horia Chiorean (hchiorea@redhat.com)
 */
public class PostgresConnectorTask extends BaseSourceTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresConnectorTask.class);
    private static final String CONTEXT_NAME = "postgres-connector-task";

    private volatile PostgresTaskContext taskContext;
    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile PostgresConnection jdbcConnection;
    private volatile PostgresConnection heartbeatConnection;
    private volatile ErrorHandler errorHandler;
    private volatile PostgresSchema schema;

    @Override
    public ChangeEventSourceCoordinator start(Configuration config) {
        LOGGER.info("task is start");
        final PostgresConnectorConfig connectorConfig = new PostgresConnectorConfig(config);
        final TopicSelector<TableId> topicSelector = PostgresTopicSelector.create(connectorConfig); // topic建立器
        final Snapshotter snapshotter = connectorConfig.getSnapshotter(); // 根据配置判断获取哪种快照 init , always

        if (snapshotter == null) {
            throw new ConnectException(
                    "Unable to load snapshotter, if using custom snapshot mode, double check your settings");
        }

        jdbcConnection = new PostgresConnection(connectorConfig.jdbcConfig()); // 关于数据库连接的相关配置
        heartbeatConnection = new PostgresConnection(connectorConfig.jdbcConfig());
        final TypeRegistry typeRegistry = jdbcConnection.getTypeRegistry(); // 数据库表基础信息
        final Charset databaseCharset = jdbcConnection.getDatabaseCharset(); // 数据库编码

        schema = new PostgresSchema(connectorConfig, typeRegistry, databaseCharset, topicSelector); // kafka 消息的封装类
        this.taskContext = new PostgresTaskContext(connectorConfig, schema, topicSelector); // task 的上下文对象
        final PostgresOffsetContext previousOffset = (PostgresOffsetContext) getPreviousOffset( // offset
                                                                                                // 上下文对象，保存之前的offset
                new PostgresOffsetContext.Loader(connectorConfig));
        final Clock clock = Clock.system();

        final SourceInfo sourceInfo = new SourceInfo(connectorConfig);
        LoggingContext.PreviousContext previousContext = taskContext.configureLoggingContext(CONTEXT_NAME);
        try {
            // Print out the server information
            SlotState slotInfo = null;
            try (PostgresConnection connection = taskContext.createConnection() // 逻辑槽，通过jdbc连接来创建
            ) {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info(connection.serverInfo().toString());
                }
                slotInfo = connection.getReplicationSlotState(connectorConfig.slotName(),
                        connectorConfig.plugin().getPostgresPluginName());
            } catch (SQLException e) {
                LOGGER.warn("unable to load info of replication slot, Debezium will try to create the slot");
            }

            if (previousOffset == null) {
                LOGGER.info("No previous offset found");
                // if we have no initial offset, indicate that to Snapshotter by passing null
                snapshotter.init(connectorConfig, null, slotInfo);
            } else {
                LOGGER.info("Found previous offset {}", sourceInfo);
                snapshotter.init(connectorConfig, previousOffset.asOffsetState(), slotInfo);
            }

            ReplicationConnection replicationConnection = null;
            SlotCreationResult slotCreatedInfo = null;
            if (snapshotter.shouldStream()) {
                /**
                 * 开始执行尝试连接数据库
                 */
                boolean shouldExport = snapshotter.exportSnapshot();
                replicationConnection = createReplicationConnection(this.taskContext, shouldExport,
                        connectorConfig.maxRetries(), connectorConfig.retryDelay()); // 创建逻辑连接

                // we need to create the slot before we start streaming if it doesn't exist
                // otherwise we can't stream back changes happening while the snapshot is taking place
                if (slotInfo == null) {
                    try {
                        slotCreatedInfo = replicationConnection.createReplicationSlot().orElse(null); // 创建逻辑槽
                    } catch (SQLException ex) {
                        throw new ConnectException(ex);
                    }
                } else {
                    slotCreatedInfo = null;
                }
            }

            queue = new ChangeEventQueue.Builder<DataChangeEvent>().pollInterval(connectorConfig.getPollInterval())
                    .maxBatchSize(connectorConfig.getMaxBatchSize()).maxQueueSize(connectorConfig.getMaxQueueSize())
                    .loggingContextSupplier(() -> taskContext.configureLoggingContext(CONTEXT_NAME)).build(); // 存放消息的队列

            errorHandler = new PostgresErrorHandler(connectorConfig.getLogicalName(), queue); // 异常拦截类

            final PostgresEventMetadataProvider metadataProvider = new PostgresEventMetadataProvider();

            Heartbeat heartbeat = Heartbeat.create(connectorConfig.getConfig(), topicSelector.getHeartbeatTopic(),
                    connectorConfig.getLogicalName(), heartbeatConnection); // 创建心跳连接

            final EventDispatcher<TableId> dispatcher = new EventDispatcher<>(connectorConfig, topicSelector, schema,
                    queue, connectorConfig.getTableFilters().dataCollectionFilter(), DataChangeEvent::new,
                    PostgresChangeRecordEmitter::updateSchema, metadataProvider, heartbeat);

            ChangeEventSourceCoordinator coordinator = new ChangeEventSourceCoordinator(previousOffset, errorHandler,
                    PostgresConnector.class, connectorConfig,
                    new PostgresChangeEventSourceFactory(connectorConfig, snapshotter, jdbcConnection, errorHandler,
                            dispatcher, clock, schema, taskContext, replicationConnection, slotCreatedInfo),
                    dispatcher, schema); // 中央调度器，负责执行数据同步

            coordinator.start(taskContext, this.queue, metadataProvider); // 开始执行

            return coordinator;
        } finally {
            previousContext.restore();
        }
    }

    /**
     * 开始导出数据，创建复制槽连接
     * 
     * @param taskContext
     * @param shouldExport
     * @param maxRetries
     * @param retryDelay
     * 
     * @return
     * 
     * @throws ConnectException
     */
    public ReplicationConnection createReplicationConnection(PostgresTaskContext taskContext, boolean shouldExport,
            int maxRetries, Duration retryDelay) throws ConnectException {
        final Metronome metronome = Metronome.parker(retryDelay, Clock.SYSTEM); // 时间策略,多久重试一次
        short retryCount = 0;
        ReplicationConnection replicationConnection = null;
        while (retryCount <= maxRetries) {
            try {
                return taskContext.createReplicationConnection(shouldExport);
            } catch (SQLException ex) {
                retryCount++;
                if (retryCount > maxRetries) {
                    LOGGER.error("Too many errors connecting to server. All {} retries failed.", maxRetries);
                    throw new ConnectException(ex);
                }

                LOGGER.warn(
                        "Error connecting to server; will attempt retry {} of {} after {} "
                                + "seconds. Exception message: {}",
                        retryCount, maxRetries, retryDelay.getSeconds(), ex.getMessage());
                try {
                    metronome.pause();
                } catch (InterruptedException e) {
                    LOGGER.warn("Connection retry sleep interrupted by exception: " + e);
                    Thread.currentThread().interrupt();
                }
            }
        }
        return replicationConnection;
    }

    @Override
    public List<SourceRecord> doPoll() throws InterruptedException {
        final List<DataChangeEvent> records = queue.poll();

        final List<SourceRecord> sourceRecords = records.stream().map(DataChangeEvent::getRecord)
                .collect(Collectors.toList());

        return sourceRecords;
    }

    @Override
    protected void doStop() {
        if (jdbcConnection != null) {
            jdbcConnection.close();
        }

        if (heartbeatConnection != null) {
            heartbeatConnection.close();
        }

        if (schema != null) {
            schema.close();
        }
    }

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    protected Iterable<Field> getAllConfigurationFields() {
        return PostgresConnectorConfig.ALL_FIELDS;
    }

    public PostgresTaskContext getTaskContext() {
        return taskContext;
    }
}
