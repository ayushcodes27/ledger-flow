package com.project.ledgerflow.controller;

import com.project.ledgerflow.dto.CreateWalletRequest;
import com.project.ledgerflow.dto.TransactionRequest;
import com.project.ledgerflow.dto.WalletResponse;
import com.project.ledgerflow.model.Wallet;
import com.project.ledgerflow.service.WalletService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequestMapping("api/v1/wallets")
@RequiredArgsConstructor
public class WalletController {

    private final WalletService walletService;

    @PostMapping
    public ResponseEntity<WalletResponse> createWallet(@Valid @RequestBody CreateWalletRequest request){
        Wallet wallet = walletService.createWallet(request.currency());
        return ResponseEntity.status(HttpStatus.CREATED).body(WalletResponse.fromEntity(wallet));
    }

    @GetMapping("api/v1/{id}")
    public ResponseEntity<WalletResponse> getWallet(@PathVariable UUID id){
        Wallet wallet = walletService.getWallet(id);
        return ResponseEntity.ok(WalletResponse.fromEntity(wallet));
    }

    @PostMapping("/{id}/credit")
    public ResponseEntity<WalletResponse> creditWallet(
            @PathVariable UUID id,
            @Valid @RequestBody TransactionRequest request) {

        Wallet updatedWallet = walletService.credit(id, request.amount());
        return ResponseEntity.ok(WalletResponse.fromEntity(updatedWallet));
    }

    @PostMapping("/{id}/debit")
    public ResponseEntity<WalletResponse> debitWallet(
            @PathVariable UUID id,
            @Valid @RequestBody TransactionRequest request) {

        Wallet updatedWallet = walletService.debit(id, request.amount());
        return ResponseEntity.ok(WalletResponse.fromEntity(updatedWallet));
    }
}
