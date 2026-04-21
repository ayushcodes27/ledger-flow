package com.project.ledgerflow.config;

import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import io.swagger.v3.oas.models.OpenAPI;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI ledgerFlowOpenAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title("LedgerFlow API")
                        .description("High-performance, event-driven FinTech ledger application demonstrating strict double-entry accounting and distributed systems resilience.")
                        .version("v1.0.0")
                        .contact(new Contact()
                                .name("Ayush")
                                .email("abcd.email@example.com"))
                        .license(new License().name("Apache 2.0").url("http://springdoc.org")));
    }
}