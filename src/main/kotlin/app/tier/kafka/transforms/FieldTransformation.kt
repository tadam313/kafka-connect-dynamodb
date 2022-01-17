package app.tier.kafka.transforms

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Struct
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

class FieldTransformation(private val fieldConfig: String) {
    val fieldName = fieldConfig.substringBefore(".*")
    private val flatten = fieldConfig.endsWith(".*")

    @Suppress("UNCHECKED_CAST")
    fun <T> apply(value: T, writeTo: MutableMap<String, AttributeValue>) {
        if (flatten && value is Map<*, *>) {
            writeTo.putAll(DynamoDBAttributeUtil.fromMap(value as Map<String, Any>))
        } else {
            writeTo[fieldName] = DynamoDBAttributeUtil.toAttributeValue(value)
        }
    }

    fun <T> apply(value: T, schema: Schema, writeTo: MutableMap<String, AttributeValue>) {
        if (flatten && value is Struct) {
            writeTo.putAll(DynamoDBAttributeUtil.fromStruct(value))
        } else {
            writeTo[fieldName] = DynamoDBAttributeUtil.toAttributeValue(value, schema)
        }
    }
}