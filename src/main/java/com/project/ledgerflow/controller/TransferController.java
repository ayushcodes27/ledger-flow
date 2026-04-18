package com.project.ledgerflow.controller;

import com.project.ledgerflow.dto.TransferRequest;
import com.project.ledgerflow.service.TransferSagaOrchestrator;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/transfers")
public class TransferController {

    private final TransferSagaOrchestrator orchestrator;

    public TransferController(TransferSagaOrchestrator orchestrator) {
        this.orchestrator = orchestrator;
    }

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
