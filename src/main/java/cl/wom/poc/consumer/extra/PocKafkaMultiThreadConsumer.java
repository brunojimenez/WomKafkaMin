package cl.wom.poc.consumer.extra;

import java.util.Properties;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Clase que permite probar varios consumidores simultaneos. Se debe probar con
 * un tópico que tenga mas de una partición para que cada consumer sea asignado
 * a alguna partición. El tópico está definido en la clase
 * PocKafkaThreadConsumer. Lanzar el producer con un número alto de mensajes
 * para que tenga efecto.
 * 
 * @author bajimenc
 */
public class PocKafkaMultiThreadConsumer {

	public static final Logger log = LoggerFactory.getLogger(PocKafkaMultiThreadConsumer.class);

	public static final String SERVERS = "localhost:9092";

	public static final String GROUP_ID = "myGroupId01";

	/**
	 * Numero de hijos a ejecutar
	 */
	public static int THREADS = 5;

	public static void main(String[] args) {

		var props = new Properties();

		// Kafka broker
		props.put("bootstrap.servers", SERVERS);

		// Deserialization
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		// Kafka broker
		props.put("group.id", GROUP_ID);

		var executorService = Executors.newFixedThreadPool(THREADS);

		for (var i = 0; i < THREADS; i++) {
			var consumer = new PocKafkaThreadConsumer(new KafkaConsumer<>(props));
			executorService.execute(consumer);
		}

	}

}
