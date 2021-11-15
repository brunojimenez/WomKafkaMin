package cl.wom.poc.consumer.extra;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PocKafkaThreadConsumer extends Thread {

	public static final Logger log = LoggerFactory.getLogger(PocKafkaThreadConsumer.class);

	private final KafkaConsumer<String, String> consumer;

	private final AtomicBoolean closed = new AtomicBoolean(false);

	public static final String TOPIC = "poc-test01";

	public PocKafkaThreadConsumer(KafkaConsumer<String, String> consumer) {
		this.consumer = consumer;
	}

	@Override
	public void run() {
		consumer.subscribe(Arrays.asList(TOPIC));
		try {
			while (!closed.get()) {
				ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
				for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
					log.info("Offset = {}, partition={}, key={}, value={}", consumerRecord.offset(),
							consumerRecord.partition(), consumerRecord.key(), consumerRecord.value());
				}
			}
		} catch (WakeupException e) {
			if (!closed.get()) {
				throw e;
			}
		} finally {
			consumer.close();
		}

	}

	public void shutdown() {
		closed.set(true);
		consumer.wakeup();
	}

}
