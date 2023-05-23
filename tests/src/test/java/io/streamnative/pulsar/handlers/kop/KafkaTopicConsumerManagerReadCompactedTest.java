/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.streamnative.pulsar.handlers.kop;

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.pulsar.client.api.SubscriptionInitialPosition.Earliest;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Same tests as KafkaTopicConsumerManagerTest but with readCompacted = true.
 */
@Slf4j
public class KafkaTopicConsumerManagerReadCompactedTest extends KafkaTopicConsumerManagerTest {

    @Override
    public boolean isReadCompacted() {
        return true;
    }

    @Override
    protected void internalSetupTopicCompaction() throws Exception {
        conf.setTopicReadCompacted(true);
        admin.namespaces().setCompactionThreshold(conf.getKafkaTenant() + "/" + conf.getKafkaNamespace(), 25);
    }

    @Test
    public void testReadCompacted() throws Exception {
        String topicName = "persistent://public/default/testReadCompacted";
        String fullTopicName = registerPartitionedTopic(topicName);
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + getKafkaBrokerPort());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        final KafkaProducer<Integer, String> producer = new KafkaProducer<>(props);

        final int numMessages = 5;
        String messagePrefix = "testReadCompacted_message_1_";
        for (int i = 0; i < numMessages; i++) {
            String message = messagePrefix + i;
            producer.send(new ProducerRecord<>(topicName, i, message)).get();
        }

        String messagePrefix2 = "testReadCompacted_message_2_";
        for (int i = 0; i < numMessages; i++) {
            String message = messagePrefix2 + i;
            producer.send(new ProducerRecord<>(topicName, i, message)).get();
        }
        producer.close();

        admin.topics().triggerCompaction(fullTopicName);
        waitForCompactionToFinish(fullTopicName, numMessages);


//        CompletableFuture<KafkaTopicConsumerManager> tcm = kafkaTopicManager.getTopicConsumerManager(fullTopicName);
//        KafkaTopicConsumerManager topicConsumerManager = tcm.get();
//
//        ManagedLedgerImpl ledger = (ManagedLedgerImpl)topicConsumerManager.getTopic().getManagedLedger();
//
//        ledger.getCursors().forEach(cursor -> {
//            log.info("cursor: {}", cursor);
//            if (cursor.getName().equals("__compaction")) {
//                log.info("cursor: {}", cursor);
//                cursor.getMarkDeletedPosition()
//            }
//        });
//        final PositionImpl previous = ledger.getPreviousPosition(PositionImpl.EARLIEST);
//        ManagedCursor cursor = ledger.newNonDurableCursor(previous,
//                "testReadCompacted_cursor",
//                CommandSubscribe.InitialPosition.Latest,
//                true);

        // before a read, first get cursor of offset.
        //Pair<ManagedCursor, Long> cursorPair = topicConsumerManager.removeCursorFuture(-1L).get();
        //ManagedCursor cursor = cursorPair.getLeft();
//
//        List<Entry> entries = cursor.readEntries(2 * numMessages);
//        entries.forEach(entry -> {
//            log.info("entry: {}", new String(entry.getData()));
//        });
//        assertEquals(entries.size(), numMessages);


//        CompletableFuture<KafkaTopicConsumerManager> tcm = kafkaTopicManager.getTopicConsumerManager(fullTopicName);
//        KafkaTopicConsumerManager topicConsumerManager = tcm.get();
//        topicConsumerManager.removeCursorFuture(0L).get();

        @Cleanup
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(newKafkaConsumerProperties());
        consumer.subscribe(Collections.singleton(topicName));

        AtomicInteger numReceived = new AtomicInteger(0);
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            if (records == null || records.isEmpty()) {
                break;
            }
            records.forEach(record -> {
                String msg = record.value();
                log.info("kafka msg: {}", msg);
                //assertTrue(msg.contains(messagePrefix2));
                numReceived.incrementAndGet();
            });
        }

        assertEquals(numReceived.get(), numMessages);
    }

    private void waitForCompactionToFinish(String fullTopicName, int numMessages) throws PulsarClientException {
        PulsarClient cl = PulsarClient.builder()
                .serviceUrl("pulsar://localhost:" + getBrokerPort())
                .build();
        Awaitility.waitAtMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            Thread.sleep(100);
            Consumer<byte[]> pcons = cl.newConsumer(Schema.BYTES)
                    .topic(fullTopicName)
                    .subscriptionMode(SubscriptionMode.NonDurable)
                    .subscriptionName("testReadCompacted-" + UUID.randomUUID())
                    .readCompacted(true)
                    .subscriptionInitialPosition(Earliest)
                    .subscribe();

            int i = 0;
            while (true) {
                Message<byte[]> m = pcons.receive(250, TimeUnit.MILLISECONDS);
                if (m == null) {
                    break;
                }
                i++;
                String msg = new String(m.getData());
                pcons.acknowledge(m);
                log.info("{} pulsar msg: {}", i, msg);
            }
            pcons.close();
            assertEquals(i, numMessages);
        });
        cl.close();
    }

}
