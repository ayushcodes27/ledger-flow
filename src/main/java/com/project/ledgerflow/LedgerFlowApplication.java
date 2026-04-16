package com.project.ledgerflow;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class LedgerFlowApplication {

    public static void main(String[] args) {
        SpringApplication.run(LedgerFlowApplication.class, args);
    }

}
