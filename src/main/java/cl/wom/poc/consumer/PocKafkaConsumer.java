package cl.wom.poc.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PocKafkaConsumer {

	public static final Logger log = LoggerFactory.getLogger(PocKafkaConsumer.class);

	public static final String SERVERS = "localhost:9092";

	public static final String TOPIC = "poc-test01";

	public static final String GROUP_ID = "myGroupId01";

	public static void main(String[] args) {

		Properties props = new Properties();

		// Kafka broker
		props.put("bootstrap.servers", SERVERS);

		// Deserialization
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		// Kafka broker
		props.put("group.id", GROUP_ID);

		// OPTIONAL - AutoCommit
		props.put("enable.auto.commit", true);

		// OPTIONAL - Offset
		props.put("auto.offset.reset", "earliest");

		// OPTIONAL - Commit Intervals
		props.put("auto.commit.intervals.ms", "1000");

		try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
			consumer.subscribe(Arrays.asList(TOPIC));
			while (true) {
				ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
				for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
					log.info("Offset = {}, partition={}, key={}, value={}", consumerRecord.offset(),
							consumerRecord.partition(), consumerRecord.key(), consumerRecord.value());
				}
			}
		} catch (Exception e) {
			log.error("Error: ", e);
		}

	}
}
