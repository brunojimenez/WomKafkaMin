package cl.wom.poc.producer.extra;

import java.util.Date;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PocKafkaProducerCallback {

	public static final Logger log = LoggerFactory.getLogger(PocKafkaProducerCallback.class);

	public static final String SERVERS = "localhost:9092";

	public static final String TOPIC = "poc-test01";

	public static void main(String[] args) {

		long startTime = System.currentTimeMillis();

		var props = new Properties();
		props.put("bootstrap.servers", SERVERS);

		// Serialization
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		props.put("acks", "all");

		try (Producer<String, String> producer = new KafkaProducer<String, String>(props)) {
			for (var i = 0; i < 10000; i++) {
				var key = String.valueOf(i);
				var value = new Date().toString();
				ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC, key, value);
				producer.send(producerRecord, new Callback() {

					@Override
					public void onCompletion(RecordMetadata metadata, Exception exception) {

						if (exception != null) {
							log.error("Error=", exception);
						}

						log.info("Offset = {}, Partition =  {}, Topic = {}", metadata.offset(), metadata.partition(),
								metadata.topic());

					}

				});
			}
			producer.flush();
		} catch (Exception e) {
			log.error("Error: ", e);
		}

		log.info("Proccessing time = {} ms", startTime);

	}

}
