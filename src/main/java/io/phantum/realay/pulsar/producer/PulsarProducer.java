package io.phantum.realay.pulsar.producer;

import io.phantum.realay.pulsar.config.PulsarConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.*;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.util.concurrent.CompletableFuture;

@Component
@Slf4j
public class PulsarProducer implements CommandLineRunner {

    private Producer producerInitialized;
    private int counter = 1;

    public void produceMessages(Producer<String> producer) {

        try {
            while(true) {
                log.info("In produceMessages :: Sending test message: " + counter);
                producer.sendAsync("Hello world " + counter++);
                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            log.error("In produceMessages :: Thread interrupted from sleep");
        }

    }

    public Producer initProducer(PulsarClient client) throws PulsarClientException {
        Producer<String> producer = client.newProducer(Schema.STRING)
                .topic(PulsarConfig.TOPIC_NAME)
                .compressionType(CompressionType.LZ4)
                .create();

        return producer;
    }

    @PreDestroy
    public void closeConnections() {
        if (producerInitialized != null) {
            try {
                producerInitialized.close();
                log.info("In closeConnections :: producer shutdown");
            } catch (PulsarClientException e) {
                log.error("In closeConnections :: error closing producer before shutdown");
            }
        }
    }

    @Override
    public void run(String... args) throws Exception {
        PulsarClient client = PulsarClient.builder().serviceUrl(PulsarConfig.SERVICE_URL).build();
        Producer producer = initProducer(client);
        this.producerInitialized = producer;

        CompletableFuture.runAsync(()-> produceMessages(producer));
    }
}
