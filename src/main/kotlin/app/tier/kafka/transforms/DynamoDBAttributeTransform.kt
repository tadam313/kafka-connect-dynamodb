package app.tier.kafka.transforms

import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.transforms.Transformation
import org.apache.kafka.connect.transforms.util.Requirements
import org.apache.kafka.connect.transforms.util.SimpleConfig
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import java.util.*

const val PURPOSE = "DynamoAttributeTransformer"
const val DEFAULT_HEADER = "DDB-ITEM"

abstract class DynamoDBAttributeTransform<R: ConnectRecord<R>>: Transformation<R> {
    private lateinit var fields: List<FieldTransformation>
    private lateinit var headerName: String

    private var configDef =  ConfigDef()
        .define("fields", ConfigDef.Type.LIST, Collections.emptyList<String>(), ConfigDef.Importance.HIGH,
            "Fields to transform to dynamo attribute descriptors ('field_name.*' to flatten maps / structs)")
        .define("header", ConfigDef.Type.STRING, DEFAULT_HEADER, ConfigDef.Importance.MEDIUM,
            "Header to put the dynamo item descriptor")

    override fun config(): ConfigDef = configDef

    override fun configure(configs: MutableMap<String, *>) {
        val config = SimpleConfig(configDef, configs)

        fields = config.getList("fields").map { FieldTransformation(it) }
        headerName = config.getString("header")
    }

    override fun close() {
    }

    protected abstract fun operatingSchema(record: R): Schema?

    protected abstract fun operatingValue(record: R): Any?

    override fun apply(record: R): R {
        val schema = operatingSchema(record)

        val dynamoDescriptor = if (schema != null)
            applySchema(record, schema)
        else
            applySchemaLess(record)

        if (dynamoDescriptor != null) {
            record.headers().add(headerName, dynamoDescriptor, null)
        }

        return record
    }

    private fun applySchemaLess(record: R): Map<String, AttributeValue>? {
        val recordMap = Requirements.requireMapOrNull(operatingValue(record), PURPOSE) ?: return null

        val headerMap = mutableMapOf<String, AttributeValue>()

        fields.forEach { transformation ->
            transformation.apply(recordMap[transformation.fieldName], headerMap)
        }

        return headerMap
    }

    private fun applySchema(record: R, schema: Schema): Map<String, AttributeValue>? {
        val struct = Requirements.requireStructOrNull(operatingValue(record), PURPOSE) ?: return null
        val headerMap = mutableMapOf<String, AttributeValue>()

        fields.forEach { transformation ->
            transformation.apply(
                struct[transformation.fieldName],
                schema.field(transformation.fieldName).schema(),
                headerMap
            )
        }

        return headerMap
    }

    class Value<R: ConnectRecord<R>>: DynamoDBAttributeTransform<R>() {
        override fun operatingSchema(record: R): Schema? = record.valueSchema()
        override fun operatingValue(record: R): Any? = record.value()
    }

    class Key<R: ConnectRecord<R>>: DynamoDBAttributeTransform<R>() {
        override fun operatingSchema(record: R): Schema? = record.keySchema()
        override fun operatingValue(record: R): Any? = record.key()
    }
}