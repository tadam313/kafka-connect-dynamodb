# Kafka connect transform DynamoDB

The project itself created to facilitate creating a Kafka sink connector for AWS DynamoDB. Two standalone components included:

- SMT (simple message transform) to transform message entries to AttributeValue for DynamoDB and place it in message header
- 0.11.x apache camel DynamoDB connector wrapped and extended

The components can be used separately, if you want to use your own connector feel free to use only the SMT. If you would like to use the whole
stack end-to-end you can do that as well.

## DynamoDBAttributeTransform

The transformer will extract specific fields from the messages and converts them to AttributeValue. It works for both key and value entries with
schema or schemaless.

```yaml
transforms.dynamo-val.type: "app.tier.kafka.transforms.DynamoDBAttributeTransform$Value" # for message value
---
transforms.dynamo-key.type: "app.tier.kafka.transforms.DynamoDBAttributeTransform$Key" # for message key
```

### Configuration options

- **fields**: comma separated list of fields to extract from messages. Use `field.*` to hoist embedded fields from composite items such as map or struct
  or use `{new_name}:{old_name}` to remap fields
- **header**: name of the header field to put AttributeValue into.

#### Example

Original message: `{ "data": {"field1": "test"}, "time": "2022-01-01" }`

With configuration:

```yaml
transforms.dynamoDB.type: "app.tier.kafka.transforms.DynamoDBAttributeTransform$Value"
transforms.dynamoDB.fields: "data.*,lastModified:time"
transforms.dynamoDB.header: "headerField"
```

Will put `{ "field1": {"S": "test"}, "lastModified": {"S": "2022-01-01"} }` entry to `headerField` header.

_Limitation_: field remapping does not work together with hoisting, that means you can not use `{new_name}:{field}:*` syntax.

_Limitation_: The message itself needs to be a composite message (has fields). You can not use this transformer with primitive values (eg. when the message is a simple string)

_Note_: lists are not supported yet

## DynamoDB connector

The connector is based on the great project called [Apache Camel](https://camel.apache.org/). However, camel connector always assumes that schema is available for message headers which is not required for this use-case.
Hence, we decided to create a connector variant based on `0.11.x` and make the header schema optional, the rest is identical to camel implementation.

The configuration as well is identical to [camel connector config](https://camel.apache.org/camel-kafka-connector/0.11.x/reference/connectors/camel-aws2-ddb-kafka-sink-connector.html).

_Note_: for convenience purposes we also included `software.amazon.awssdk:sts` package which lets you use web identity tokens with DefaultCredentialsProvider

### Example (strimzi) connector config (put + delete):

```yaml
dynamodb-put:
  class: app.tier.kafka.DynamoSinkConnectorExtended
  tasksMax: 2
  config:
    camel.sink.endpoint.region: "eu-central-1"
    camel.sink.endpoint.useDefaultCredentialsProvider: "true"
    topics: "..."
    transforms: "dynamoDB"
    transforms.dynamoDB.type: "app.tier.kafka.transforms.DynamoDBAttributeTransform$Value"
    transforms.dynamoDB.fields: "..."
    transforms.dynamoDB.header: "CamelHeader.CamelAwsDdbItem"
# ... or delete ...
dynamodb-delete:
  class: app.tier.kafka.DynamoSinkConnectorExtended
  tasksMax: 2
  config:
    camel.sink.endpoint.region: "eu-central-1"
    camel.sink.endpoint.operation: "DeleteItem"
    camel.sink.endpoint.useDefaultCredentialsProvider: "true"
    topics: ""
    transforms: "dynamoDB"
    transforms.dynamoDB.type: "app.tier.kafka.transforms.DynamoDBAttributeTransform$Key"
    transforms.dynamoDB.header: "CamelHeader.CamelAwsDdbKey"
```
