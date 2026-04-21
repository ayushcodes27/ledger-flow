package com.project.ledgerflow.service;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import static com.project.ledgerflow.service.AbstractIntegrationTest.postgres;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
class LedgerFlowApplicationTests extends AbstractIntegrationTest {

    @MockitoBean
    private KafkaTemplate<String, String> kafkaTemplate;

    @Test
    void contextLoads() {
        // If the test reaches this point, it means Postgres, Kafka, and Redis
        // spun up successfully and Spring Boot connected to them.
        assertThat(postgres.isRunning()).isTrue();
        assertThat(kafka.isRunning()).isTrue();
        assertThat(redis.isRunning()).isTrue();
    }

}
