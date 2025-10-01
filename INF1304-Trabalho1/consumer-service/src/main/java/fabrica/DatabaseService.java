package fabrica;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.PreparedStatement;

public class DatabaseService {

    private static final String DB_URL = "jdbc:sqlite:" + System.getenv("DB_PATH");

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
