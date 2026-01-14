package com.jpmc.midascore.component;

import java.util.Optional;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component; // Import Incentive

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.repository.UserRepository;

@Component
public class KafkaTransactionListener {

    private final UserRepository userRepository;
    private final TransactionRepository transactionRepository;
    private final IncentiveApiClient incentiveApiClient; // <--- 1. Inject Client

    public KafkaTransactionListener(UserRepository userRepository, 
                                    TransactionRepository transactionRepository,
                                    IncentiveApiClient incentiveApiClient) {
        this.userRepository = userRepository;
        this.transactionRepository = transactionRepository;
        this.incentiveApiClient = incentiveApiClient;
    }

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core-group")
    public void listen(Transaction transaction) {
        // 1. Validate Sender and Recipient exist
        Optional<UserRecord> senderOpt = userRepository.findById(transaction.getSenderId());
        Optional<UserRecord> recipientOpt = userRepository.findById(transaction.getRecipientId());

        if (senderOpt.isEmpty() || recipientOpt.isEmpty()) {
            return;
        }

        UserRecord sender = senderOpt.get();
        UserRecord recipient = recipientOpt.get();

        // 2. Validate Sender has enough balance
        if (sender.getBalance() < transaction.getAmount()) {
            return;
        }

        // --- NEW LOGIC START ---
        
        // 3. Call the API to get the incentive
        Incentive incentive = incentiveApiClient.getIncentive(transaction);
        float incentiveAmount = incentive.getAmount();

        // 4. Update Balances
        // Sender only loses the transaction amount
        sender.setBalance(sender.getBalance() - transaction.getAmount());
        
        // Recipient gets transaction amount + incentive
        recipient.setBalance(recipient.getBalance() + transaction.getAmount() + incentiveAmount);

        userRepository.save(sender);
        userRepository.save(recipient);

        
        TransactionRecord record = new TransactionRecord(sender, recipient, transaction.getAmount(), incentiveAmount);
        transactionRepository.save(record);
        

    }
}