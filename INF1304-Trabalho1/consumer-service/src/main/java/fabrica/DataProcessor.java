package fabrica;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fabrica.WebSocketServer;
import fabrica.DatabaseService;

import org.json.JSONObject;

/**
 * Classe responsável por consumir dados de sensores a partir do Kafka,
 * processar as mensagens recebidas, armazenar em banco de dados e 
 * transmitir via WebSocket para o frontend.
 */
public class DataProcessor {
    /** Tópico Kafka a ser consumido (definido pela variável de ambiente KAFKA_TOPIC). */
    private static final String TOPIC = System.getenv().getOrDefault("KAFKA_TOPIC", "dados-sensores");
    /** Endereço dos brokers Kafka (definido pela variável de ambiente KAFKA_BROKERS). */
    private static final String BOOTSTRAP_SERVERS = System.getenv().getOrDefault("KAFKA_BROKERS", "kafka1:9092");
    /** Grupo de consumidores Kafka (definido pela variável de ambiente KAFKA_CONSUMER_GROUP). */
    private static final String GROUP_ID = System.getenv().getOrDefault("KAFKA_CONSUMER_GROUP", "consumer-service");

    /** Logger para monitoramento da aplicação. */
    private static final Logger logger = LoggerFactory.getLogger(DataProcessor.class);

    /**
     * Método principal que inicializa o consumidor Kafka, 
     * processa as mensagens recebidas e envia para o WebSocket.
     *
     * @param args Argumentos da linha de comando (não utilizados).
     */
    public static void main(String[] args) {
        logger.info("Starting DataProcessor Consumer.");

        // Cria instância do banco
        DatabaseService dbService = new DatabaseService();

        // Configura propriedades do consumidor Kafka
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("group.id", GROUP_ID);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 3000);
        props.put(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, 5000);
        props.put(ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, 10000);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC));

        // Inicia servidor WebSocket
        WebSocketServer.startServer();

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    logger.info("Received message: " + record.value());

                    // Envia para frontend
                    WebSocketServer.broadcast(record.value());

                    // Processa JSON
                    try {
                        JSONObject json = new JSONObject(record.value());
                        String sensorId = json.optString("sensorId", "unknown");
                        double temperatura = json.optDouble("temperatura", -1);
                        double vibracao = json.optDouble("vibracao", -1);

                        boolean isAlert = (temperatura > 30 || vibracao > 8);
                        dbService.salvarLog(sensorId, temperatura, vibracao, isAlert);
                        if (isAlert) {
                            logger.warn("⚠️ ALERTA: Sensor " + sensorId +
                                        " -> Temp: " + temperatura + ", Vib: " + vibracao);
                        }

                    } catch (Exception e) {
                        logger.error("Erro ao processar mensagem JSON: " + record.value(), e);
                    }
                }
            }
        } finally {
            consumer.close();
        }
    }
}
