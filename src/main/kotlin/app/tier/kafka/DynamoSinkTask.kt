package app.tier.kafka

import org.apache.camel.CamelContext
import org.apache.camel.Endpoint
import org.apache.camel.Exchange
import org.apache.camel.ProducerTemplate
import org.apache.camel.impl.DefaultCamelContext
import org.apache.camel.kafkaconnector.CamelSinkConnectorConfig
import org.apache.camel.kafkaconnector.aws2ddb.CamelAws2ddbSinkTask
import org.apache.camel.kafkaconnector.utils.CamelKafkaConnectMain
import org.apache.camel.kafkaconnector.utils.TaskHelper
import org.apache.camel.support.DefaultExchange
import org.apache.camel.util.StringHelper
import org.apache.kafka.connect.data.Decimal
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.header.Header
import org.apache.kafka.connect.sink.SinkRecord
import org.slf4j.LoggerFactory

const val LOCAL_URL = "direct:start"

class DynamoSinkTask: CamelAws2ddbSinkTask() {
    private val logger = LoggerFactory.getLogger("DynamoSinkTask")
    private lateinit var cms: CamelKafkaConnectMain
    private lateinit var producer: ProducerTemplate
    private lateinit var localEndpoint: Endpoint

    override fun start(props: MutableMap<String, String>?) {
        try {
            logger.info("Starting CamelSinkTask connector task")
            val actualProps = TaskHelper.combineDefaultAndLoadedProperties(
                defaultConfig, props
            )
            val config = getCamelSinkConnectorConfig(actualProps)

            val camelContext: CamelContext = DefaultCamelContext()

            val remoteUrl = config.getString(CamelSinkConnectorConfig.CAMEL_SINK_URL_CONF) ?: TaskHelper.buildUrl(
                camelContext,
                actualProps,
                config.getString(CamelSinkConnectorConfig.CAMEL_SINK_COMPONENT_CONF),
                "camel.sink.endpoint.",
                "camel.sink.path."
            )

            val marshaller = config.getString(CamelSinkConnectorConfig.CAMEL_SINK_MARSHAL_CONF)
            val unmarshaller = config.getString(CamelSinkConnectorConfig.CAMEL_SINK_UNMARSHAL_CONF)
            val size = config.getInt(CamelSinkConnectorConfig.CAMEL_CONNECTOR_AGGREGATE_SIZE_CONF)
            val timeout = config.getLong(CamelSinkConnectorConfig.CAMEL_CONNECTOR_AGGREGATE_TIMEOUT_CONF)
            val maxRedeliveries =
                config.getInt(CamelSinkConnectorConfig.CAMEL_CONNECTOR_ERROR_HANDLER_MAXIMUM_REDELIVERIES_CONF)
            val redeliveryDelay =
                config.getLong(CamelSinkConnectorConfig.CAMEL_CONNECTOR_ERROR_HANDLER_REDELIVERY_DELAY_CONF)
            val errorHandler = config.getString(CamelSinkConnectorConfig.CAMEL_CONNECTOR_ERROR_HANDLER_CONF)
            val idempotencyEnabled =
                config.getBoolean(CamelSinkConnectorConfig.CAMEL_CONNECTOR_IDEMPOTENCY_ENABLED_CONF)
            val expressionType =
                config.getString(CamelSinkConnectorConfig.CAMEL_CONNECTOR_IDEMPOTENCY_EXPRESSION_TYPE_CONF)
            val expressionHeader =
                config.getString(CamelSinkConnectorConfig.CAMEL_CONNECTOR_IDEMPOTENCY_EXPRESSION_HEADER_CONF)
            val memoryDimension =
                config.getInt(CamelSinkConnectorConfig.CAMEL_CONNECTOR_IDEMPOTENCY_MEMORY_DIMENSION_CONF)
            val idempotentRepositoryType =
                config.getString(CamelSinkConnectorConfig.CAMEL_CONNECTOR_IDEMPOTENCY_REPOSITORY_TYPE_CONF)
            val idempotentRepositoryKafkaTopic =
                config.getString(CamelSinkConnectorConfig.CAMEL_CONNECTOR_IDEMPOTENCY_KAFKA_TOPIC_CONF)
            val idempotentRepositoryBootstrapServers =
                config.getString(CamelSinkConnectorConfig.CAMEL_CONNECTOR_IDEMPOTENCY_KAFKA_BOOTSTRAP_SERVERS_CONF)
            val idempotentRepositoryKafkaMaxCacheSize =
                config.getInt(CamelSinkConnectorConfig.CAMEL_CONNECTOR_IDEMPOTENCY_KAFKA_MAX_CACHE_SIZE_CONF)
            val idempotentRepositoryKafkaPollDuration =
                config.getInt(CamelSinkConnectorConfig.CAMEL_CONNECTOR_IDEMPOTENCY_KAFKA_POLL_DURATION_CONF)
            val headersRemovePattern =
                config.getString(CamelSinkConnectorConfig.CAMEL_CONNECTOR_REMOVE_HEADERS_PATTERN_CONF)

            cms = CamelKafkaConnectMain.builder(LOCAL_URL, remoteUrl)
                .withProperties(actualProps)
                .withUnmarshallDataFormat(unmarshaller)
                .withMarshallDataFormat(marshaller)
                .withAggregationSize(size)
                .withAggregationTimeout(timeout)
                .withErrorHandler(errorHandler)
                .withMaxRedeliveries(maxRedeliveries)
                .withRedeliveryDelay(redeliveryDelay)
                .withIdempotencyEnabled(idempotencyEnabled)
                .withExpressionType(expressionType)
                .withExpressionHeader(expressionHeader)
                .withMemoryDimension(memoryDimension)
                .withIdempotentRepositoryType(idempotentRepositoryType)
                .withIdempotentRepositoryTopicName(idempotentRepositoryKafkaTopic)
                .withIdempotentRepositoryKafkaServers(idempotentRepositoryBootstrapServers)
                .withIdempotentRepositoryKafkaMaxCacheSize(idempotentRepositoryKafkaMaxCacheSize)
                .withIdempotentRepositoryKafkaPollDuration(idempotentRepositoryKafkaPollDuration)
                .withHeadersExcludePattern(headersRemovePattern)
                .build(camelContext)
            cms.start()
            producer = cms.producerTemplate
            localEndpoint = cms.camelContext.getEndpoint(LOCAL_URL)
            logger.info("CamelSinkTask connector task started")
        } catch (e: Exception) {
            throw ConnectException("Failed to create and start Camel context", e)
        }
    }

    override fun put(sinkRecords: MutableCollection<SinkRecord>?) {
        for (record in sinkRecords!!) {
            val exchange: Exchange = DefaultExchange(producer.camelContext)
            exchange.message.body = record.value()
            exchange.message.setHeader(KAFKA_RECORD_KEY_HEADER, record.key())
            for (header in record.headers()) {
                if (header.key().startsWith(HEADER_CAMEL_PREFIX)) {
                    mapHeader(header, HEADER_CAMEL_PREFIX, exchange.message.headers)
                } else if (header.key().startsWith(PROPERTY_CAMEL_PREFIX)) {
                    mapHeader(header, PROPERTY_CAMEL_PREFIX, exchange.properties)
                }
            }
            logger.debug("Sending exchange {} to {}", exchange.exchangeId, LOCAL_URL)
            producer.send(localEndpoint, exchange)

            if (exchange.isFailed) {
                logger.warn("A delivery has failed and the error reporting is enabled. Sending record to the DLQ")
            }
        }
    }

    override fun stop() {
        logger.info("Stopping CamelSinkTask connector task")
        try {
            cms.stop()
        } catch (e: java.lang.Exception) {
            throw ConnectException("Failed to stop Camel context", e)
        } finally {
            logger.info("CamelSinkTask connector task stopped")
        }
    }

    private fun mapHeader(header: Header, prefix: String, destination: MutableMap<String, Any>) {
        val key = StringHelper.after(header.key(), prefix, header.key())
        val schema = header.schema()

        if (schema != null && schema.type() == Schema.BYTES_SCHEMA.type() && schema.name() == Decimal.LOGICAL_NAME) {
            destination[key] = Decimal.toLogical(schema, header.value() as ByteArray)
        } else {
            destination[key] = header.value()
        }
    }
}