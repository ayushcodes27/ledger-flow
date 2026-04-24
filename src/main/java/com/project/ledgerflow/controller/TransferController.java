package com.project.ledgerflow.controller;

import com.project.ledgerflow.dto.TransferRequest;
import com.project.ledgerflow.service.TransferSagaOrchestrator;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/transfers")
public class TransferController {

    private final TransferSagaOrchestrator orchestrator;

    public TransferController(TransferSagaOrchestrator orchestrator) {
        this.orchestrator = orchestrator;
    }

    @Operation(summary = "Initiate a money transfer", description = "Atomically initiates a transfer between two wallets using the Saga pattern.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "202", description = "Transfer accepted and processing asynchronously",
                    content = { @Content(mediaType = "application/json", schema = @Schema(implementation = Map.class)) }),
            @ApiResponse(responseCode = "400", description = "Invalid wallet IDs or insufficient funds", content = @Content),
            @ApiResponse(responseCode = "409", description = "Optimistic locking failure (concurrent modification)", content = @Content)
    })
    @PostMapping
    public ResponseEntity<Map<String, Object>> initiateTransfer(@Valid @RequestBody TransferRequest request){

        //Initiate the SAGA
        UUID transactionId = orchestrator.initiateTransfer(
                request.sourceWalletId(),
                request.targetWalletId(),
                request.amount()
        );

        // 3. Return 202 Accepted
        return ResponseEntity.accepted().body(Map.of(
                "transactionId", transactionId,
                "message", "Transfer accepted and is processing."
        ));
    }
}
