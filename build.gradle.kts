import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.6.10"
    java
    distribution
}

group = "app.tier.kafka"
version = "${properties["app.tier.ddb_transformer.version"]}"

var camelVersion = "0.11.0"
var awsSDKVersion = "2.17.110"
var kafkaVersion = "3.0.0"

repositories {
    mavenCentral()
}

val deployerJars = configurations.create("deployerJars")

dependencies {
    api("org.apache.kafka:connect-transforms:$kafkaVersion")
    api("org.apache.kafka:connect-api:$kafkaVersion")

    implementation("org.apache.camel.kafkaconnector:camel-kafka-connector:$camelVersion")
    implementation("org.apache.camel.kafkaconnector:camel-aws2-ddb-kafka-connector:$camelVersion")
    implementation("software.amazon.awssdk:dynamodb:$awsSDKVersion")
    implementation("org.slf4j:slf4j-simple:1.7.32")

    testImplementation("org.junit.jupiter:junit-jupiter:5.8.2")
    testImplementation(kotlin("test"))

    deployerJars("software.amazon.awssdk:dynamodb:$awsSDKVersion")
    deployerJars("org.apache.camel.kafkaconnector:camel-aws2-ddb-kafka-connector:$camelVersion")
    deployerJars("org.apache.camel.kafkaconnector:camel-kafka-connector:$camelVersion")
}

distributions {
    main {
        contents {
            from(tasks.jar.get(), deployerJars)
        }
    }
}

tasks.distTar {
    compression = Compression.GZIP
}

tasks.test {
    useJUnitPlatform()
}

tasks.withType<KotlinCompile>() {
    kotlinOptions.jvmTarget = "1.8"
}
