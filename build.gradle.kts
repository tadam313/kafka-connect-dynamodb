import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.6.10"
    java
}

group = "app.tier.kafka"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

val deployerJars = configurations.create("deployerJars")

dependencies {
    api("org.apache.kafka:connect-transforms:3.0.0")
    api("org.apache.kafka:connect-api:3.0.0")

    implementation("org.apache.camel.kafkaconnector:camel-kafka-connector:0.11.0")
    implementation("org.apache.camel.kafkaconnector:camel-aws2-ddb-kafka-connector:0.11.0")
    implementation("software.amazon.awssdk:dynamodb:2.17.110")
    implementation("org.slf4j:slf4j-simple:1.7.32")

    testImplementation("org.junit.jupiter:junit-jupiter:5.8.2")
    testImplementation(kotlin("test"))

    deployerJars("software.amazon.awssdk:dynamodb:2.17.110")
    deployerJars("org.apache.camel.kafkaconnector:camel-aws2-ddb-kafka-connector:0.11.0")
    deployerJars("org.apache.camel.kafkaconnector:camel-kafka-connector:0.11.0")
}

tasks.test {
    useJUnitPlatform()
}

tasks.withType<KotlinCompile>() {
    kotlinOptions.jvmTarget = "1.8"
}

task<Copy>("copyToLibs") {
    from(deployerJars)
    into("$buildDir/libs")
}

tasks.build {
    dependsOn("copyToLibs")
}