// package com.jpmc.midascore;

// import org.junit.jupiter.api.Test;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;
// import org.springframework.beans.factory.annotation.Autowired;
// import org.springframework.boot.test.context.SpringBootTest;
// import org.springframework.kafka.test.context.EmbeddedKafka;
// import org.springframework.test.annotation.DirtiesContext;

// @SpringBootTest
// @DirtiesContext
// @EmbeddedKafka(partitions = 1, brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
// public class TaskFourTests {
//     static final Logger logger = LoggerFactory.getLogger(TaskFourTests.class);

//     @Autowired
//     private KafkaProducer kafkaProducer;

//     @Autowired
//     private UserPopulator userPopulator;

//     @Autowired
//     private FileLoader fileLoader;

//     @Test
//     void task_four_verifier() throws InterruptedException {
//         userPopulator.populate();
//         String[] transactionLines = fileLoader.loadStrings("/test_data/alskdjfh.fhdjsk");
//         for (String transactionLine : transactionLines) {
//             kafkaProducer.send(transactionLine);
//         }
//         Thread.sleep(2000);


//         logger.info("----------------------------------------------------------");
//         logger.info("----------------------------------------------------------");
//         logger.info("----------------------------------------------------------");
//         logger.info("use your debugger to find out what wilbur's balance is after all transactions are processed");
//         logger.info("kill this test once you find the answer");
//         while (true) {
//             Thread.sleep(20000);
//             logger.info("...");
//         }
//     }
// }
package com.jpmc.midascore;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.repository.UserRepository;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
public class TaskFourTests {
    static final Logger logger = LoggerFactory.getLogger(TaskFourTests.class);

    @Autowired
    private KafkaProducer kafkaProducer;

    @Autowired
    private UserPopulator userPopulator;

    @Autowired
    private FileLoader fileLoader;

    @Autowired
    private UserRepository userRepository; // <--- 1. Inject Repository

    @Test
    void task_four_verifier() throws InterruptedException {
        userPopulator.populate();
        String[] transactionLines = fileLoader.loadStrings("/test_data/alskdjfh.fhdjsk");
        
        for (String transactionLine : transactionLines) {
            kafkaProducer.send(transactionLine);
        }
        
        // Wait for Kafka and API processing
        Thread.sleep(2000);

        // --- 2. Print Result ---
        UserRecord wilbur = userRepository.findByName("wilbur");

        logger.info("==========================================================");
        logger.info("==========================================================");
        logger.info("!!! RESULT FOUND !!!");
        
        if (wilbur != null) {
            logger.info("✅ WILBUR BALANCE: " + wilbur.getBalance());
            logger.info("👇 ROUNDED DOWN:   " + (int) wilbur.getBalance());
        } else {
            logger.info("❌ User 'wilbur' not found!");
        }

        logger.info("==========================================================");
        logger.info("==========================================================");
    }
}