package com.project.ledgerflow.controller;

import com.project.ledgerflow.dto.CreateWalletRequest;
import com.project.ledgerflow.dto.LedgerResponse;
import com.project.ledgerflow.dto.TransactionRequest;
import com.project.ledgerflow.dto.WalletResponse;
import com.project.ledgerflow.model.Wallet;
import com.project.ledgerflow.repository.LedgerEntryRepository;
import com.project.ledgerflow.service.WalletService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("api/v1/wallets")
@RequiredArgsConstructor
public class WalletController {

    private final WalletService walletService;
    private final LedgerEntryRepository ledgerEntryRepository;

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

    @GetMapping("/{id}/ledger")
    public ResponseEntity<List<LedgerResponse>> getLedger(@PathVariable UUID id) {
        List<LedgerResponse> ledger = ledgerEntryRepository.findByWalletIdOrderByCreatedAtDesc(id)
                .stream()
                .map(LedgerResponse::fromEntity)
                .toList();

        return ResponseEntity.ok(ledger);
    }
}
