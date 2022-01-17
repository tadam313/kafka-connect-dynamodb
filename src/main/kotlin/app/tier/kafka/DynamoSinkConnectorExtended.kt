package app.tier.kafka

import org.apache.camel.kafkaconnector.aws2ddb.CamelAws2ddbSinkConnector
import org.apache.kafka.connect.connector.Task

class DynamoSinkConnectorExtended: CamelAws2ddbSinkConnector() {
    override fun taskClass(): Class<out Task> = DynamoSinkTask::class.java
}