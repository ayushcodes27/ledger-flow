package com.project.ledgerflow.controller;

import com.project.ledgerflow.dto.TransactionRequest;
import com.project.ledgerflow.dto.WalletResponse;
import com.project.ledgerflow.model.Wallet;
import com.project.ledgerflow.service.WalletService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1/wallets")
@RequiredArgsConstructor
@Tag(name = "Wallet Management", description = "Endpoints for managing user wallets and balances")
public class WalletController {

    private final WalletService walletService;

    @Operation(summary = "Create a new wallet", description = "Initializes a new wallet with zero balance for the specified currency.")
    @PostMapping
    public ResponseEntity<WalletResponse> createWallet(@RequestParam String currency) {
        Wallet wallet = walletService.createWallet(currency);
        return ResponseEntity.status(HttpStatus.CREATED).body(mapToResponse(wallet));
    }

    @Operation(summary = "Get wallet by ID", description = "Retrieves current balance and currency for a specific wallet.")
    @GetMapping("/{id}")
    public ResponseEntity<WalletResponse> getWallet(@PathVariable UUID id) {
        Wallet wallet = walletService.getWallet(id);
        return ResponseEntity.ok(mapToResponse(wallet));
    }

    @Operation(summary = "Credit a wallet", description = "Increases the wallet balance. Requires an Idempotency-Key for safety.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Wallet credited successfully"),
            @ApiResponse(responseCode = "409", description = "Idempotency key already used")
    })
    @PostMapping("/{id}/credit")
    public ResponseEntity<WalletResponse> credit(
            @PathVariable UUID id,
            @Valid @RequestBody TransactionRequest request,
            @Parameter(description = "Unique key to prevent duplicate processing")
            @RequestHeader(value = "Idempotency-Key") String idempotencyKey) {
        Wallet updatedWallet = walletService.credit(id, request.amount(), idempotencyKey, null);
        return ResponseEntity.ok(mapToResponse(updatedWallet));
    }

    @Operation(summary = "Debit a wallet", description = "Decreases the wallet balance if sufficient funds exist. Requires an Idempotency-Key.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Wallet debited successfully"),
            @ApiResponse(responseCode = "400", description = "Insufficient funds"),
            @ApiResponse(responseCode = "409", description = "Idempotency key already used")
    })
    @PostMapping("/{id}/debit")
    public ResponseEntity<WalletResponse> debit(
            @PathVariable UUID id,
            @Valid @RequestBody TransactionRequest request,
            @Parameter(description = "Unique key to prevent duplicate processing")
            @RequestHeader(value = "Idempotency-Key") String idempotencyKey) {
        Wallet updatedWallet = walletService.debit(id, request.amount(), idempotencyKey, null);
        return ResponseEntity.ok(mapToResponse(updatedWallet));
    }

    private WalletResponse mapToResponse(Wallet wallet) {
        return new WalletResponse(wallet.getId(), wallet.getBalance(), wallet.getCurrency());
    }
}
