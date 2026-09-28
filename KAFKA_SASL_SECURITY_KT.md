# Kafka SASL SCRAM-SHA-256 Security Knowledge Transfer (KT) Document
**Architecture:** Apache Kafka 4.3.1 (KRaft Mode - ZooKeeper-less)  
**Project:** `bSmartKafkaDataAdapter` (Spring Boot 3.x / Java 21)  
**Target Broker:** `192.168.70.71:9092` (Host: `ubuntu-OptiPlex-3050`)  
**Internal Admin Listener:** `127.0.0.1:9094`  
**KRaft Controller Listener:** `127.0.0.1:9093`  
**Kafka Installation Directory:** `/application/kafka_mdm/kafka`  
**Security Protocol:** `SASL_PLAINTEXT`  
**Authentication Mechanism:** `SCRAM-SHA-256`  
**Application User / Credentials:** `bcitstest` / `BcitsKafka#2026`  
**Admin User:** `bcits` / `Bcits@Kafka#26!`  

---

## Table of Contents
1. [Executive Summary & Purpose](#1-executive-summary--purpose)
2. [Security Architecture (KRaft Mode)](#2-security-architecture-kraft-mode)
3. [Prerequisites & Java Environment Setup](#3-prerequisites--java-environment-setup)
4. [Broker-Side Configuration Step-by-Step](#4-broker-side-configuration-step-by-step)
   - 4.1. [Create Broker JAAS Config (`kafka_server_jaas.conf`)](#41-create-broker-jaas-config-kafka_server_jaasconf)
   - 4.2. [Set JVM Parameter (`KAFKA_OPTS`)](#42-set-jvm-parameter-kafka_opts)
   - 4.3. [Configure `server.properties` for KRaft & SASL](#43-configure-serverproperties-for-kraft--sasl)
   - 4.4. [Start / Restart Kafka Broker Service](#44-start--restart-kafka-broker-service)
5. [User Credential Management via CLI (`SCRAM-SHA-256`)](#5-user-credential-management-via-cli-scram-sha-256)
   - 5.1. [Register Application User Credentials](#51-register-application-user-credentials)
   - 5.2. [Verify Registered Credentials](#52-verify-registered-credentials)
6. [CLI Verification & Testing from Server](#6-cli-verification--testing-from-server)
   - 6.1. [Create Test Client Config (`client-test.properties`)](#61-create-test-client-config-client-testproperties)
   - 6.2. [Test Topic Listing via SASL](#62-test-topic-listing-via-sasl)
   - 6.3. [Test Producing & Consuming with Authentication](#63-test-producing--consuming-with-authentication)
7. [Spring Boot Client Implementation (`bSmartKafkaDataAdapter`)](#7-spring-boot-client-implementation-bsmartkafkadataadapter)
   - 7.1. [`application.properties` Configuration](#71-applicationproperties-configuration)
   - 7.2. [`KafkaConsumerConfig.java` Consumer Factory](#72-kafkaconsumerconfigjava-consumer-factory)
   - 7.3. [Suppressing Verbose Kafka Startup Logs](#73-suppressing-verbose-kafka-startup-logs)
8. [Troubleshooting Guide & Common Pitfalls](#8-troubleshooting-guide--common-pitfalls)
9. [Pre-Deployment & Production Checklist](#9-pre-deployment--production-checklist)

---

## 1. Executive Summary & Purpose

In the Smart Meter Data Integration architecture, high-frequency meter events stream into Kafka. Securing the Kafka cluster using **SASL (Simple Authentication and Security Layer) with SCRAM-SHA-256** ensures:

1. **Authentication:** Only authorized applications with valid salt/hash cryptographic credentials can connect to port `9092`.
2. **Password Security:** Unlike `PLAIN` text, `SCRAM-SHA-256` uses salted challenge-response authentication, preventing credential interception even without TLS.
3. **KRaft Mode Efficiency:** Managed natively inside Kafka's Raft metadata partition without external ZooKeeper dependencies.
4. **Data Isolation:** Isolates application consumer groups and ensures only authorized clients read meter data payloads and write to Dead Letter Queues (DLQ).

---

## 2. Security Architecture (KRaft Mode)

```
+-----------------------------------------------------------------------------------------+
|                      bSmartKafkaDataAdapter (Spring Boot Client)                         |
|                                                                                         |
|  +-----------------------------------------------------------------------------------+  |
|  |                             application.properties                                |  |
|  |  - kafka.bootstrap-servers = 192.168.70.71:9092                                   |  |
|  |  - kafka.security.protocol = SASL_PLAINTEXT                                       |  |
|  |  - kafka.sasl.mechanism = SCRAM-SHA-256                                           |  |
|  |  - kafka.sasl.jaas.config = ScramLoginModule (user="bcitstest", pass="***")       |  |
|  +-----------------------------------------------------------------------------------+  |
|                                         |                                               |
|                                         v                                               |
|  +-----------------------------------------------------------------------------------+  |
|  |                       DefaultKafkaConsumerFactory                                 |  |
|  +-----------------------------------------------------------------------------------+  |
+-----------------------------------------|-----------------------------------------------+
                                          |
                                          | 1. SASL SCRAM-SHA-256 Handshake
                                          v
+-----------------------------------------------------------------------------------------+
|                Kafka Broker 4.3.1: ubuntu-OptiPlex-3050 (192.168.70.71)                 |
|                                                                                         |
|  [Port 9092] SASL_PLAINTEXT ───▶ SASL Authentication (ScramLoginModule)                |
|  [Port 9094] PLAINTEXT      ───▶ Local Admin & Management Port                          |
|  [Port 9093] CONTROLLER     ───▶ KRaft Raft Quorum Controller                           |
|                                                                                         |
|  +-----------------------------------------------------------------------------------+  |
|  |                         KRaft Metadata Cluster Storage                            |  |
|  |   - Stores SCRAM-SHA-256 User Hashes (/metadata)                                  |  |
|  |   - User: bcitstest (iterations: 4096)                                            |  |
|  +-----------------------------------------------------------------------------------+  |
+-----------------------------------------------------------------------------------------+
```

---

## 3. Prerequisites & Java Environment Setup

Apache Kafka 4.x / 4.3.1 requires **Java 17 or Java 21**. Running on Java 8 produces `UnsupportedClassVersionError (class file version 61.0)`.

### Configure Java 21 Permanently for the User:
```bash
# Add Java 21 to user bash profile
echo 'export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64' >> ~/.bashrc
echo 'export PATH=$JAVA_HOME/bin:$PATH' >> ~/.bashrc
source ~/.bashrc

# Verify
java -version
```

---

## 4. Broker-Side Configuration Step-by-Step

All operations are executed under `/application/kafka_mdm/kafka` on `ubuntu-OptiPlex-3050`.

### 4.1. Create Broker JAAS Config (`kafka_server_jaas.conf`)
Path: `/application/kafka_mdm/kafka/config/kafka_server_jaas.conf`

```bash
cat << 'EOF' > /application/kafka_mdm/kafka/config/kafka_server_jaas.conf
KafkaServer {
    org.apache.kafka.common.security.scram.ScramLoginModule required
    username="bcits"
    password="Bcits@Kafka#26!";
};

Client {
    org.apache.kafka.common.security.scram.ScramLoginModule required
    username="bcits"
    password="Bcits@Kafka#26!";
};
EOF
```

### 4.2. Set JVM Parameter (`KAFKA_OPTS`)
Before starting the broker, point Kafka to the JAAS configuration:

```bash
export KAFKA_OPTS="-Djava.security.auth.login.config=/application/kafka_mdm/kafka/config/kafka_server_jaas.conf"
```

### 4.3. Configure `server.properties` for KRaft & SASL
Edit `/application/kafka_mdm/kafka/config/server.properties`:

```properties
############################# Socket Server Settings #############################
listeners=SASL_PLAINTEXT://0.0.0.0:9092,PLAINTEXT://127.0.0.1:9094,CONTROLLER://:9093
advertised.listeners=SASL_PLAINTEXT://192.168.70.71:9092,PLAINTEXT://127.0.0.1:9094
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,SASL_PLAINTEXT:SASL_PLAINTEXT

############################# SASL Settings #############################
sasl.enabled.mechanisms=SCRAM-SHA-256
sasl.mechanism.inter.broker.protocol=PLAINTEXT
inter.broker.listener.name=PLAINTEXT
```

> [!IMPORTANT]
> In KRaft mode, do **NOT** define `security.inter.broker.protocol` when `inter.broker.listener.name` is present, or Kafka will throw a `ConfigException`.

### 4.4. Start / Restart Kafka Broker Service
```bash
# 1. Terminate any previous instances
pkill -9 -f kafka

# 2. Set JAAS config
export KAFKA_OPTS="-Djava.security.auth.login.config=/application/kafka_mdm/kafka/config/kafka_server_jaas.conf"

# 3. Start Kafka in daemon mode
cd /application/kafka_mdm/kafka/bin
./kafka-server-start.sh -daemon /application/kafka_mdm/kafka/config/server.properties

# 4. Verify Listening Ports
ss -tulpn | grep -E "9092|9093|9094"
```

Expected Output:
```text
tcp LISTEN 0 50 *:9093 users:(("java",pid=...,fd=...))
tcp LISTEN 0 50 *:9092 users:(("java",pid=...,fd=...))
tcp LISTEN 0 50 127.0.0.1:9094 users:(("java",pid=...,fd=...))
```

---

## 5. User Credential Management via CLI (`SCRAM-SHA-256`)

In Kafka 4.x / KRaft mode, user credentials are created dynamically via `--bootstrap-server` (using the internal admin port `localhost:9094`).

### 5.1. Register Application User Credentials
```bash
/application/kafka_mdm/kafka/bin/kafka-configs.sh \
  --bootstrap-server localhost:9094 \
  --entity-type users \
  --entity-name bcitstest \
  --alter \
  --add-config 'SCRAM-SHA-256=[password=BcitsKafka#2026]'
```

### 5.2. Verify Registered Credentials
```bash
/application/kafka_mdm/kafka/bin/kafka-configs.sh \
  --bootstrap-server localhost:9094 \
  --entity-type users \
  --describe
```

Expected Output:
```text
SCRAM credential configs for user-principal 'bcitstest' are SCRAM-SHA-256=iterations=4096
```

---

## 6. CLI Verification & Testing from Server

### 6.1. Create Test Client Config (`/tmp/client-test.properties`)
```bash
cat << 'EOF' > /tmp/client-test.properties
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-256
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="bcitstest" password="BcitsKafka#2026";
EOF
```

### 6.2. Test Topic Listing via SASL on Port `9092`
```bash
/application/kafka_mdm/kafka/bin/kafka-topics.sh \
  --bootstrap-server 192.168.70.71:9092 \
  --command-config /tmp/client-test.properties \
  --list
```

### 6.3. Test Producing & Consuming with Authentication
```bash
# 1. Produce a test message
echo "Test payload from KT session" | /application/kafka_mdm/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server 192.168.70.71:9092 \
  --producer.config /tmp/client-test.properties \
  --topic BSMART_DAILYLOAD_TOPIC

# 2. Consume the message
/application/kafka_mdm/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server 192.168.70.71:9092 \
  --consumer.config /tmp/client-test.properties \
  --topic BSMART_DAILYLOAD_TOPIC \
  --from-beginning \
  --max-messages 1
```

---

## 7. Spring Boot Client Implementation (`bSmartKafkaDataAdapter`)

### 7.1. `application.properties` Configuration
File: `src/main/resources/application.properties`

```properties
# Kafka Broker Address (Port 9092 with SASL)
kafka.bootstrap-servers=192.168.70.71:9092
kafka.group-id=consumer-group-1
kafka.topics=BSMART_DAILYLOAD_TOPIC
kafka.dlqTopics=BSMART_DAILYLOAD_TOPIC_DLQ
project=BCITS

# SASL SCRAM-SHA-256 Security Settings
kafka.security.protocol=SASL_PLAINTEXT
kafka.sasl.mechanism=SCRAM-SHA-256
kafka.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="bcitstest" password="BcitsKafka#2026";

# Retry and Backoff Settings
kafka.retry.max-attempts=3
kafka.retry.backoff-interval=2000

# Logging: Suppress verbose Kafka startup dumps & KIP-848 banner
logging.level.org.apache.kafka=WARN
logging.level.org.springframework.kafka=WARN
```

### 7.2. `KafkaConsumerConfig.java` Consumer Factory
File: `src/main/java/com/bcits/responseDataModel/KafkaConsumerConfig.java`

```java
@Configuration
public class KafkaConsumerConfig {

    @Bean
    public ConsumerFactory<String, String> consumerFactory(KafkaConfigProperties configProperties) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, configProperties.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, configProperties.getGroupId());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 200);
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 32768);
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000);
        props.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 65536);

        // Security Properties
        if (configProperties.getSecurityProtocol() != null && !configProperties.getSecurityProtocol().isEmpty()) {
            props.put("security.protocol", configProperties.getSecurityProtocol());
        }
        if (configProperties.getSaslMechanism() != null && !configProperties.getSaslMechanism().isEmpty()) {
            props.put("sasl.mechanism", configProperties.getSaslMechanism());
        }
        if (configProperties.getSaslJaasConfig() != null && !configProperties.getSaslJaasConfig().isEmpty()) {
            props.put("sasl.jaas.config", configProperties.getSaslJaasConfig());
        }

        return new DefaultKafkaConsumerFactory<>(props);
    }
    
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            ConsumerFactory<? super String, ? super String> consumerFactory,
            KafkaConfigProperties configProperties) {

        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(new DefaultErrorHandler(
                new FixedBackOff(configProperties.getBackoffInterval(),
                        configProperties.getMaxRetryAttempts())));

        return factory;
    }
}
```

### 7.3. Suppressing Verbose Kafka Startup Logs
By default, Kafka Client prints large property dumps and the KIP-848 informational banner at `INFO` level. Adding the following in `application.properties` cleans the logs:

```properties
logging.level.org.apache.kafka=WARN
logging.level.org.springframework.kafka=WARN
```

---

## 8. Troubleshooting Guide & Common Pitfalls

| Issue / Error | Root Cause | Resolution |
| :--- | :--- | :--- |
| `UnsupportedClassVersionError: ... class file version 61.0` | Active Java version is Java 8, but Kafka 4.x requires Java 17/21. | Switch to Java 21 via `export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64` and add to `~/.bashrc`. |
| `ConfigException: Only one of inter.broker.listener.name and security.inter.broker.protocol should be set` | KRaft mode conflict when both old and new inter-broker configs are defined. | Remove `security.inter.broker.protocol` and keep `inter.broker.listener.name=PLAINTEXT`. |
| `KafkaException: Failed to acquire lock on file .lock` | Previous Kafka process didn't terminate cleanly. | Run `pkill -9 -f kafka`, check `jps` is empty, then restart. |
| `DUPLICATE_BROKER_REGISTRATION` in logs | Broker restarted quickly before KRaft heartbeat expired. | Normal during fast restarts; broker auto-recovers within seconds. |
| `These configurations '[spring.json.trusted.packages]' were supplied but are not used yet` | `JsonDeserializer.TRUSTED_PACKAGES` set in props while using `StringDeserializer`. | Remove `props.put(JsonDeserializer.TRUSTED_PACKAGES, "*")` from `KafkaConsumerConfig.java`. |
| `SaslAuthenticationException: Authentication failed: Invalid username or password` | User credentials mismatch in JAAS config or SCRAM hash not created. | Verify credentials with `kafka-configs.sh --describe` and check client JAAS string. |

---

## 9. Pre-Deployment & Production Checklist

- [x] **Java Runtime:** Java 21 configured permanently in `~/.bashrc`.
- [x] **Broker JAAS File:** Created at `/application/kafka_mdm/kafka/config/kafka_server_jaas.conf`.
- [x] **KAFKA_OPTS Configured:** JVM parameter points to broker JAAS file.
- [x] **KRaft Listeners:** `listeners=SASL_PLAINTEXT://0.0.0.0:9092,PLAINTEXT://127.0.0.1:9094,CONTROLLER://:9093`.
- [x] **SCRAM User Registered:** `bcitstest` registered in KRaft with `SCRAM-SHA-256`.
- [x] **CLI SASL Verified:** Tested topic listing and producer/consumer via port `9092`.
- [x] **Client Application Configured:** `application.properties` updated with `SASL_PLAINTEXT` and `SCRAM-SHA-256`.
- [x] **Logging Cleaned:** Verbose startup property dumps and KIP-848 banner suppressed.
