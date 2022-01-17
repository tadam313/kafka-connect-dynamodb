package app.tier.kafka.transforms

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Struct
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

class FieldTransformation(config: String) {
    val fieldName: String
    private val flatten = config.endsWith(".*")
    private val destinationFieldName: String

    init {
        val fieldConfig = config.substringBefore(".*").split(":")

        if (fieldConfig.size > 2) {
            throw IllegalArgumentException("Field format: {field_name} or {header_field:record_field}")
        }

        destinationFieldName = fieldConfig.first()
        fieldName = fieldConfig.last()
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> apply(value: T, writeTo: MutableMap<String, AttributeValue>) {
        if (flatten && value is Map<*, *>) {
            writeTo.putAll(DynamoDBAttributeUtil.fromMap(value as Map<String, Any>))
        } else {
            writeTo[destinationFieldName] = DynamoDBAttributeUtil.toAttributeValue(value)
        }
    }

    fun <T> apply(value: T, schema: Schema, writeTo: MutableMap<String, AttributeValue>) {
        if (flatten && value is Struct) {
            writeTo.putAll(DynamoDBAttributeUtil.fromStruct(value))
        } else {
            writeTo[destinationFieldName] = DynamoDBAttributeUtil.toAttributeValue(value, schema)
        }
    }

    companion object {
        fun fromSchema(schema: Schema) = schema.fields().map { FieldTransformation(it.name()) }
        fun <T> fromMap(map: Map<String, T>) = map.keys.map { FieldTransformation(it) }
    }
}