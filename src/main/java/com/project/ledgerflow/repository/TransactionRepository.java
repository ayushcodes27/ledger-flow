package com.project.ledgerflow.repository;

import com.project.ledgerflow.entity.Transaction;
import com.project.ledgerflow.entity.enums.TransactionStatus;
import com.project.ledgerflow.service.TransferExecutionCommand;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;
import java.util.UUID;

@Repository
public interface TransactionRepository extends JpaRepository<Transaction, UUID> {

    @Query("""
        select new com.project.ledgerflow.service.TransferExecutionCommand(
            t.id,
            t.sourceWalletId,
            t.targetWalletId,
            t.amount
        )
        from Transaction t
        where t.id = :transactionId
    """)
    Optional<TransferExecutionCommand> findExecutionCommandById(@Param("transactionId") UUID transactionId);

    @Modifying
    @Transactional
    @Query("""
        update Transaction t
        set t.status = :status
        where t.id = :transactionId
    """)
    int updateStatusById(@Param("transactionId") UUID transactionId, @Param("status") TransactionStatus status);
}
