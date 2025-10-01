package fabrica;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.PreparedStatement;

public class DatabaseService {

    private static final String DB_PATH = System.getenv().getOrDefault("DB_PATH", "alerts.db");
    private static final String DB_URL = "jdbc:sqlite:" + DB_PATH;

    public DatabaseService() {
        try (Connection conn = DriverManager.getConnection(DB_URL);
             Statement stmt = conn.createStatement()) {
            
            // Cria a tabela se não existir
            String sql = "CREATE TABLE IF NOT EXISTS alerts (" +
                         "id INTEGER PRIMARY KEY AUTOINCREMENT," +
                         "sensor_id TEXT NOT NULL," +
                         "temperature REAL," +
                         "vibration REAL," +
                         "timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)";
            stmt.execute(sql);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void salvarAlerta(String sensorId, double temperatura, double vibracao) {
        String sql = "INSERT INTO alerts(sensor_id, temperature, vibration) VALUES (?, ?, ?)";
        try (Connection conn = DriverManager.getConnection(DB_URL);
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            pstmt.setString(1, sensorId);
            pstmt.setDouble(2, temperatura);
            pstmt.setDouble(3, vibracao);
            pstmt.executeUpdate();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
