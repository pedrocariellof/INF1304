package br.com.fabrica;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.Date;
import com.google.gson.JsonObject;

public class SensorProducer {
    private static final Logger logger = LoggerFactory.getLogger(SensorProducer.class);
    private static final Random random = new Random();

    public static void main(String[] args) throws Exception {
        String topic = "dados-sensores";
        String kafkaBrokers = System.getenv("KAFKA_BROKERS"); // Ex: "kafka:9092"

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        logger.info("Iniciando SensorProducer. Enviando dados para o tópico {}", topic);

        while (true) {
            JsonObject sensorData = new JsonObject();
            sensorData.addProperty("sensorId", "sensor-" + (random.nextInt(5) + 1));
            sensorData.addProperty("timestamp", new Date().toString());
            sensorData.addProperty("temperatura", 20 + random.nextInt(15)); // 20-35 ºC
            sensorData.addProperty("vibracao", random.nextDouble() * 10);   // 0.0 - 10.0

            String message = sensorData.toString();

            logger.info("Enviando: {}", message);
            producer.send(new ProducerRecord<>(topic, message));

            TimeUnit.SECONDS.sleep(3); // espera 3 segundos entre leituras
        }
    }
}
