import com.google.protobuf.gradle.id

plugins {
    id("java")
    id("com.google.protobuf") version "0.9.3"
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("io.grpc:grpc-api:1.55.1")
    implementation("com.hivemq:hivemq-mqtt-client:1.3.1")
    implementation("org.slf4j:slf4j-api:2.0.7")


    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.assertj:assertj-core:3.24.2")
    testImplementation("org.slf4j:slf4j-simple:2.0.7")
}

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

tasks {
    test {
        useJUnitPlatform()
    }
}


tasks.test {
    useJUnitPlatform()
}

// Integration testing setup
tasks.register<Test>("integrationTest") {
    testClassesDirs = sourceSets["integrationTest"].output.classesDirs
    classpath = sourceSets["integrationTest"].runtimeClasspath
    shouldRunAfter(tasks["test"])
    useJUnitPlatform()
}

sourceSets {
    create("intTest") {
        compileClasspath += sourceSets.main.get().output + sourceSets.test.get().output
        runtimeClasspath += sourceSets.main.get().output + sourceSets.test.get().output
    }
}

val intTestImplementation by configurations.getting {
    extendsFrom(configurations.getByName("implementation"))
}
configurations["intTestRuntimeOnly"].extendsFrom(configurations.runtimeOnly.get())

dependencies {
    intTestImplementation("javax.annotation:javax.annotation-api:1.3.2")
    intTestImplementation("org.slf4j:slf4j-simple:2.0.7")
    intTestImplementation("org.assertj:assertj-core:3.24.2")

    intTestImplementation("io.grpc:grpc-protobuf:1.55.1")
    intTestImplementation("io.grpc:grpc-stub:1.55.1")
    intTestImplementation("io.grpc:grpc-netty-shaded:1.55.1")
    // Testcontainers
    intTestImplementation("org.testcontainers:testcontainers:1.16.3")
    intTestImplementation("org.testcontainers:junit-jupiter:1.16.3")
    intTestImplementation("com.hivemq:hivemq-testcontainer-junit5:2.0.0")
}

// Protobuf plugin configuration
protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:3.19.1"
    }
    plugins {
        id("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:1.43.1"
        }
    }
    generateProtoTasks {
        all().forEach {
            it.plugins {
                id("grpc")
            }
        }
    }
}
