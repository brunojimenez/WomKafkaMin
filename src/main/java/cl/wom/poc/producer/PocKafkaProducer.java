package cl.wom.poc.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PocKafkaProducer {

	public static final Logger log = LoggerFactory.getLogger(PocKafkaProducer.class);

	public static final String SERVERS = "localhost:9092";

	public static final String TOPIC = "poc-test01";

	public static void main(String[] args) {

		long startTime = System.currentTimeMillis();

		Properties props = new Properties();

		// Kafka broker
		// https://kafka.apache.org/documentation/#producerconfigs_bootstrap.servers
		props.put("bootstrap.servers", SERVERS);

		// Serialization
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		// OPTIONAL - Acknowledge (string - default=all)
		// 0 - No wait for any acknowledgement (no retries effect)
		// 1 - Almost leader ack
		// N - Almost N brokers with ack
		// -1/all - All brokers with ack (in-sync)
		// https://kafka.apache.org/documentation/#producerconfigs_acks
		props.put("acks", "all");

		// OPTIONAL - Buffer Memory (long, default=33554432)
		// https://kafka.apache.org/documentation/#producerconfigs_buffer.memory
		props.put("buffer.memory", "33554432");

		// OPTIONAL - Retries (int, defaul=2147483647)
		// https://kafka.apache.org/documentation/#producerconfigs_retries
		props.put("retries", "2147483647");

		// OPTIONAL - Batch Size (int - default=16384)
		// https://kafka.apache.org/documentation/#producerconfigs_batch.size
		props.put("batch.size", "16384");

		// OPTIONAL - Linger (long - default=0)
		// https://kafka.apache.org/documentation/#producerconfigs_linger.ms
		props.put("linger.ms", "6");

		// OPTIONAL - delivery.timeout.ms (int - default=120000 (2 minutes))
		// delivery.timeout.ms >= linger.ms + request.timeout.ms
		// https://kafka.apache.org/documentation/#producerconfigs_delivery.timeout.mss
		props.put("delivery.timeout.ms", "120000");

		try (Producer<String, String> producer = new KafkaProducer<String, String>(props)) {
			for (var i = 0; i < 10; i++) {
				var key = String.valueOf(i);
				var value = "AAD";
				ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC, key, value);
				producer.send(producerRecord);
			}
			producer.flush();
		} catch (Exception e) {
			log.error("Error: ", e);
		}

		log.info("Proccessing time = {} ms", startTime);

	}

}
