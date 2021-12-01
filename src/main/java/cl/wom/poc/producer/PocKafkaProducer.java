package cl.wom.poc.producer;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Date;
import java.util.Properties;
import java.util.Scanner;

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

		props.put("bootstrap.servers", SERVERS);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		try (Producer<String, String> producer = new KafkaProducer<String, String>(props)) {
			for (var i = 0; i < 10000; i++) {
				var key = String.valueOf(i);
				var value = new String(Files.readAllBytes(Paths.get("src/main/resources/message.txt")));
				log.info("message: \n{}", value);

				ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC, key, value);
				producer.send(producerRecord, (metadata, exception) -> {
					if (exception != null) {
						log.error("Error = ", exception);
					}
					log.info("Partition = {}, Offset = {}, key = {}, value = {}", metadata.partition(),
							metadata.offset(), key, value);
				});
			}
			producer.flush();
		} catch (Exception e) {
			log.error("Error: ", e);
		}

		log.info("Proccessing time = {} ms", startTime);

	}

}
