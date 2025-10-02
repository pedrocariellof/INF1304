package fabrica;

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

/**
 * Classe responsável por simular sensores de fábrica que enviam
 * leituras de temperatura e vibração para um tópico no Apache Kafka.
 * 
 * As mensagens são geradas de forma aleatória e enviadas continuamente
 * em formato JSON, com intervalo de 3 segundos entre cada envio.
 *  
 */
public class SensorProducer {

    /** Logger utilizado para registrar as operações do produtor. */
    private static final Logger logger = LoggerFactory.getLogger(SensorProducer.class);

    /** Gerador de números aleatórios para simular dados de sensores. */
    private static final Random random = new Random();

    /**
     * Método principal que inicializa o produtor Kafka, gera dados de sensores
     * aleatórios em formato JSON e envia para o tópico {@code dados-sensores}.
     *
     * @param args Argumentos de linha de comando (não utilizados).
     * @throws Exception Caso ocorra erro ao enviar mensagens para o Kafka.
     */
    public static void main(String[] args) throws Exception {
        String topic = "dados-sensores";
        String kafkaBrokers = System.getenv("KAFKA_BROKERS"); // Ex: "kafka:9092"

        // Configura propriedades do produtor Kafka
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        logger.info("Iniciando SensorProducer. Enviando dados para o tópico {}", topic);

        // Loop infinito gerando e enviando dados simulados
        while (true) {
            JsonObject sensorData = new JsonObject();
            sensorData.addProperty("sensorId", "sensor-" + (random.nextInt(5) + 1));
            sensorData.addProperty("timestamp", new Date().toString());
            sensorData.addProperty("temperatura", 20 + random.nextInt(15)); // 20-35 ºC
            sensorData.addProperty("vibracao", random.nextDouble() * 10);   // 0.0 - 10.0

            String message = sensorData.toString();

            logger.info("Enviando: {}", message);
            producer.send(new ProducerRecord<>(topic, message));

            // Aguarda 3 segundos antes de enviar nova leitura
            TimeUnit.SECONDS.sleep(3);
        }
    }
}
