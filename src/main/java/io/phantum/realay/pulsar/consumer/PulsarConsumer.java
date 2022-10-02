package io.phantum.realay.pulsar.consumer;

import io.phantum.realay.pulsar.config.PulsarConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.*;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.nio.charset.Charset;
import java.util.concurrent.CompletableFuture;

@Component
@Slf4j
public class PulsarConsumer implements CommandLineRunner {

    private Consumer consumerInitialized;

    public void consumeMessages(Consumer<String> consumer) {
        try {
            while(true) {
                Message<String> msg = consumer.receive();
                consumer.acknowledgeAsync(msg);
                String data = new String(msg.getData(), Charset.defaultCharset());
                log.info("In consumeMessages :: received a message: " + data);
                log.info("In consumeMessages :: received a message from topic: " + msg.getTopicName());
            }
        } catch (PulsarClientException e) {
            log.error("In consumeMessages :: error receiving message");
        }
    }

    public Consumer initConsumer(PulsarClient client) throws PulsarClientException {
        Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topic(PulsarConfig.TOPIC_NAME)
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionName("Test")
                .subscribe();

        return consumer;
    }

    @PreDestroy
    public void closeConnections()  {

        if(consumerInitialized != null) {
            try {
                consumerInitialized.close();
                log.info("In closeConnections :: consumer shutdown");
            } catch (PulsarClientException e) {
                log.error("In closeConnections :: error closing consumer before shutdown");
            }
        }
    }

    @Override
    public void run(String... args) throws Exception {
        PulsarClient client = PulsarClient.builder().serviceUrl(PulsarConfig.SERVICE_URL).build();
        Consumer consumer = initConsumer(client);
        this.consumerInitialized = consumer;

        CompletableFuture.runAsync(()-> consumeMessages(consumer));
    }
}

