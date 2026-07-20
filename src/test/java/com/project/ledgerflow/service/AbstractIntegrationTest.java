package com.project.ledgerflow.service;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.utility.DockerImageName;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
public abstract class AbstractIntegrationTest {

    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:15-alpine");

    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0"));

    static Network network = Network.newNetwork();

    static ToxiproxyContainer toxiproxy = new ToxiproxyContainer("ghcr.io/shopify/toxiproxy:2.11.0")
            .withNetwork(network);

    static GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379)
            .withNetwork(network)
            .withNetworkAliases("redis");

    protected static ToxiproxyContainer.ContainerProxy redisProxy;

    static {
        postgres.start();
        kafka.start();
        toxiproxy.start();
        redis.start();
        redisProxy = toxiproxy.getProxy(redis, 6379);
    }

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("spring.data.redis.host", toxiproxy::getHost);
        registry.add("spring.data.redis.port", () -> toxiproxy.getMappedPort(redisProxy.getOriginalProxyPort()));
        registry.add("spring.data.redis.password", () -> "");
        registry.add("spring.task.scheduling.enabled", () -> false);
        registry.add("spring.kafka.listener.auto-startup", () -> false);
    }

    // This base class ensures all containers are up before the Spring Context loads.
    // DynamicPropertySource injects the container endpoints into the application context.
}
