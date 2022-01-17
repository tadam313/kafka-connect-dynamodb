package app.tier.kafka.transforms

import app.tier.kafka.transforms.DynamoDBAttributeUtil.attr
import org.apache.kafka.connect.sink.SinkRecord
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.junit.jupiter.api.Test
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

internal class DynamoDBAttributeTransformTest {
    private val attributeTransformer = DynamoDBAttributeTransform.Value<SinkRecord>()

    @BeforeEach
    fun setup() {
        attributeTransformer.configure(mutableMapOf<String, String>())
    }

    @Test
    fun `test tombstone without schema`() {
        val record = SinkRecord("test", 0, null, "test", null, null, 0)

        val newRecord = attributeTransformer.apply(record)

        assertNull(newRecord.value())
        assertNull(newRecord.valueSchema())
    }

    @Test
    fun `test tombstone with schema`() {
        val testSchema = SchemaBuilder.struct()
            .field("test", Schema.STRING_SCHEMA)
            .build()

        val record = SinkRecord("test", 0, null, "test", testSchema, null, 0)

        val newRecord = attributeTransformer.apply(record)

        assertNull(newRecord.value())

        assertEquals(testSchema, newRecord.valueSchema())
    }

    @Test
    fun `test fields transform schemaless`() {
        val testValue = givenSchemalessTestData()

        val expected = mapOf(
            "test_map" to attr().m(mapOf(
                "test_inner_bool" to AttributeValue.builder().bool(false).build(),
                "test_num" to attr().n("1").build(),
                "test_string" to attr().s("test").build(),
            )).build(),
            "test_number" to attr().n("23").build()
        )

        val record = SinkRecord("test", 0, null, "test", null, testValue, 0)

        attributeTransformer.configure(mutableMapOf("fields" to "test_map,test_number"))
        val actual = attributeTransformer.apply(record).headers().lastWithName(DEFAULT_HEADER)

        assertEquals(expected, actual.value())
        assertNull(actual.schema())
    }

    @Test
    fun `test fields transform schemaless flatten`() {
        val testValue = givenSchemalessTestData()

        val expected = mapOf(
            "test_inner_bool" to AttributeValue.builder().bool(false).build(),
            "test_num" to attr().n("1").build(),
            "test_string" to attr().s("test").build(),
            "test_number" to attr().n("23").build()
        )

        val record = SinkRecord("test", 0, null, "test", null, testValue, 0)

        attributeTransformer.configure(mutableMapOf("fields" to "test_map.*,test_number"))
        val actual = attributeTransformer.apply(record).headers().lastWithName(DEFAULT_HEADER)

        assertEquals(expected, actual.value())
        assertNull(actual.schema())
    }

    @Test
    fun `test fields transform with schema`() {
        val testValue = givenTestDataWithSchema()
        val record = SinkRecord("test", 0, null, "test", testValue.schema(), testValue, 0)

        attributeTransformer.configure(mutableMapOf("fields" to "test_struct,test_string"))
        val actual = attributeTransformer.apply(record).headers().lastWithName(DEFAULT_HEADER)

        val expected = mapOf(
            "test_struct" to attr().m(mapOf(
                "test_boolean" to AttributeValue.builder().bool(false).build(),
            )).build(),
            "test_string" to attr().s("test").build()
        )

        assertEquals(expected, actual.value())
        assertNull(actual.schema())
    }

    @Test
    fun `test fields transform with schema flatten`() {
        val testValue = givenTestDataWithSchema()
        val record = SinkRecord("test", 0, null, "test", testValue.schema(), testValue, 0)

        attributeTransformer.configure(mutableMapOf("fields" to "test_struct.*,test_string"))
        val actual = attributeTransformer.apply(record).headers().lastWithName(DEFAULT_HEADER)

        val expected = mapOf(
            "test_boolean" to AttributeValue.builder().bool(false).build(),
            "test_string" to attr().s("test").build()
        )

        assertEquals(expected, actual.value())
        assertNull(actual.schema())
    }

    @Test
    fun `test field rename`() {
        val testValue = givenTestDataWithSchema()
        val record = SinkRecord("test", 0, null, "test", testValue.schema(), testValue, 0)

        attributeTransformer.configure(mutableMapOf("fields" to "number_new:test_number"))
        val actual = attributeTransformer.apply(record).headers().lastWithName(DEFAULT_HEADER)

        val expected = mapOf(
            "number_new" to attr().n("13").build()
        )

        assertEquals(expected, actual.value())
        assertNull(actual.schema())
    }

    private fun givenTestDataWithSchema(): Struct {
        val testInnerSchema = SchemaBuilder.struct()
            .field("test_boolean", Schema.BOOLEAN_SCHEMA)
            .build()

        val testSchema = SchemaBuilder.struct()
            .field("test_struct", testInnerSchema)
            .field("test_string", Schema.STRING_SCHEMA)
            .field("test_number", Schema.INT32_SCHEMA)
            .build()

        return Struct(testSchema)
            .put("test_struct", Struct(testInnerSchema).put("test_boolean", false))
            .put("test_string", "test")
            .put("test_number", 13)
    }

    private fun givenSchemalessTestData() = mapOf(
        "test_map" to mapOf(
            "test_inner_bool" to false,
            "test_num" to 1,
            "test_string" to "test",
        ),
        "test_str" to "test",
        "test_number" to 23
    )

}