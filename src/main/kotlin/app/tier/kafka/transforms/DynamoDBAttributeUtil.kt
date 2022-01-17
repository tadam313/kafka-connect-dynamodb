package app.tier.kafka.transforms

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Struct
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import java.util.function.Function
import java.util.stream.Collectors

object DynamoDBAttributeUtil {
    private val numberSchemaTypes = listOf(
        Schema.Type.FLOAT32, Schema.Type.FLOAT64,
        Schema.Type.INT8, Schema.Type.INT16, Schema.Type.INT32, Schema.Type.INT64
    )

    fun fromMap(value: Map<String, Any>): Map<String, AttributeValue> = value.mapValues { toAttributeValue(it.value) }

    fun fromStruct(value: Struct): Map<String, AttributeValue> = value.schema().fields().map {
            it
        }.stream().collect(Collectors.toMap({ it.name() }) {
            toAttributeValue(value[it.name()], it.schema())
        })

    fun attr(): AttributeValue.Builder = AttributeValue.builder()

    @Suppress("UNCHECKED_CAST")
    fun toAttributeValue(value: Any?): AttributeValue {
        if (value == null) return attr().nul(true).build()
        if (value is String) return attr().s(value).build()
        if (value is Number) return attr().n(value.toString()).build()
        if (value is Boolean) return attr().bool(value).build()
        if (value is Map<*, *>) return attr().m(fromMap(value as Map<String, Any>)).build()

        throw UnsupportedOperationException("Transformation is not implemented for $value")
    }

    @Suppress("UNCHECKED_CAST")
    fun toAttributeValue(value: Any?, schema: Schema): AttributeValue {
        if (value == null) return attr().nul(true).build()
        if (schema.type() == Schema.Type.STRING) return attr().s(value as String).build()
        if (numberSchemaTypes.contains(schema.type())) return attr().n(value as String) .n(value.toString()).build()
        if (schema.type() == Schema.Type.BOOLEAN) return attr().bool(value as Boolean).build()
        if (schema.type() == Schema.Type.MAP) return attr().m(fromMap(value as Map<String, Any>)).build()
        if (schema.type() == Schema.Type.STRUCT) return attr().m(fromStruct(value as Struct)).build()

        throw UnsupportedOperationException("Transformation is not implemented for $value with schema ${schema.type()}")
    }
}