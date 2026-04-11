package com.project.ledgerflow.service;

import com.project.ledgerflow.model.Wallet;
import com.project.ledgerflow.repository.IdempotencyKeyRepository;
import com.project.ledgerflow.repository.LedgerEntryRepository;
import com.project.ledgerflow.repository.WalletRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.junit.jupiter.api.Assertions.*;


@ExtendWith(MockitoExtension.class) //
class WalletServiceTest {

    @Mock
    private WalletRepository walletRepository;
    @Mock
    private LedgerEntryRepository ledgerEntryRepository;
    @Mock
    private IdempotencyKeyRepository idempotencyKeyRepository;

    @InjectMocks
    private WalletService walletService;

    @Test
    void debit_Successful_WhenSufficientFunds() {
        // ARRANGE
        UUID walletId = UUID.randomUUID();
        String idempotencyKey = "test-key-123";
        Wallet mockWallet = Wallet.builder()
                .id(walletId)
                .balance(new BigDecimal("100.00"))
                .currency("USD")
                .build();

        // Tell the mocks how to behave
        when(idempotencyKeyRepository.existsById(idempotencyKey)).thenReturn(false);
        when(walletRepository.findById(walletId)).thenReturn(Optional.of(mockWallet));
        // When save is called, just return whatever was passed into it
        when(walletRepository.save(any(Wallet.class))).thenAnswer(invocation -> invocation.getArgument(0));

        // ACT (Call the real method)
        Wallet result = walletService.debit(walletId, new BigDecimal("40.00"), idempotencyKey);

        // ASSERT (Verify the results)
        assertEquals(new BigDecimal("60.00"), result.getBalance()); // 100 - 40 = 60
        verify(ledgerEntryRepository, times(1)).save(any()); // Verify a receipt was created
    }

    @Test
    void debit_ThrowsException_WhenInsufficientFunds() {
        // ARRANGE
        UUID walletId = UUID.randomUUID();
        String idempotencyKey = "test-key-456";
        Wallet mockWallet = Wallet.builder()
                .id(walletId)
                .balance(new BigDecimal("20.00"))
                .currency("USD")
                .build();

        when(idempotencyKeyRepository.existsById(idempotencyKey)).thenReturn(false);
        when(walletRepository.findById(walletId)).thenReturn(Optional.of(mockWallet));

        // ACT & ASSERT
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            walletService.debit(walletId, new BigDecimal("50.00"), idempotencyKey);
        });

        assertTrue(exception.getMessage().contains("Insufficient funds"));

        // Verify that NO database saves happened because the transaction aborted
        verify(walletRepository, never()).save(any());
        verify(ledgerEntryRepository, never()).save(any());
    }
}