package fabrica;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.PreparedStatement;

/**
 * Serviço responsável por gerenciar a persistência de dados no banco SQLite.
 * A classe cria automaticamente a tabela {@code alerts} caso não exista
 * e fornece métodos para salvar logs de sensores.
 */
public class DatabaseService {

    /** Caminho do banco de dados SQLite, definido pela variável de ambiente DB_PATH. */
    private static final String DB_URL = "jdbc:sqlite:" + System.getenv("DB_PATH");

    /**
     * Construtor que inicializa a conexão com o banco e garante
     * que a tabela {@code alerts} exista.
     */
    public DatabaseService() {
        try (Connection conn = DriverManager.getConnection(DB_URL);
             Statement stmt = conn.createStatement()) {
            
            String sql = "CREATE TABLE IF NOT EXISTS alerts (" +
             "id INTEGER PRIMARY KEY AUTOINCREMENT," +
             "sensor_id TEXT NOT NULL," +
             "temperature REAL," +
             "vibration REAL," +
             "timestamp DATETIME DEFAULT CURRENT_TIMESTAMP," +
             "is_alert BOOLEAN DEFAULT 0)";

            stmt.execute(sql);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Insere um novo registro de log na tabela {@code alerts}.
     *
     * @param sensorId   Identificador do sensor.
     * @param temperatura Valor da temperatura registrada.
     * @param vibracao    Valor da vibração registrada.
     * @param isAlert     Indica se o registro gerou alerta (true) ou não (false).
     */
    public void salvarLog(String sensorId, double temperatura, double vibracao, boolean isAlert) {
        String sql = "INSERT INTO alerts(sensor_id, temperature, vibration,is_alert) VALUES (?, ?, ?, ?)";
        try (Connection conn = DriverManager.getConnection(DB_URL);
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            pstmt.setString(1, sensorId);
            pstmt.setDouble(2, temperatura);
            pstmt.setDouble(3, vibracao);
            pstmt.setBoolean(4, isAlert);
            pstmt.executeUpdate();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
