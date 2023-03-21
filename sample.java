package Hoad.hoad;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException; 

public class Procedural {

    private static final String CSV_FILE_PATH = "/home/mohith-int/orderline_April_may.csv";

    private static final String JDBC_URL = "jdbc:mysql://172.16.16.104:3306/ccev2_syscmhuat?allowPublicKeyRetrieval=true";
    private static final String JDBC_USER = "ccev2_syscmhuat";
    private static final String JDBC_PASSWORD = "Cc_3nGag3_Uat";

    public static void main(String[] args) throws IOException, IOException {
        try (Connection conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASSWORD)) {
            updateCampaignExecutionTable(conn);
            updateContactHistoryTable(conn);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void updateCampaignExecutionTable(Connection conn) throws SQLException, FileNotFoundException, IOException {
        String sql = "UPDATE cmh_campaign_execution_t1 a " +
                     "INNER JOIN tmp_soq_nullupdated b " +
                     "ON (a.COMMUNICATION_ID = b.COMMUNICATION_ID " +
                     "AND a.TEAM = b.TEAM " +
                     "AND a.CAMPAIGN_NAME = b.CAMPAIGN_NAME) " +
                     "SET a.SOQ_GOVERNANCE = ?, " +
                     "a.CAMPAIGN_TYPE_GOVERNANCE = ?, " +
                     "a.COMMUNICATION_TYPE_GOVERNANCE = ? " +
                     "WHERE a.CAMPAIGN_DATE = '2023-03-08' " +
                     "AND a.SOQ_GOVERNANCE IS NULL";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            try (BufferedReader reader = new BufferedReader(new FileReader(CSV_FILE_PATH))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] values = line.split(",");
                    stmt.setString(1, values[0]);
                    stmt.setString(2, values[1]);
                    stmt.setString(3, values[2]);
                    stmt.addBatch();
                }
                stmt.executeBatch();
            }
        }
    }

    private static void updateContactHistoryTable(Connection conn) throws SQLException {
        String sql = "UPDATE ccev2_usrcmhprod.cmh_contact_history " +
                     "SET CONTACT_STATUS = 'PROCESSED' " +
                     "WHERE CONTACT_STATUS = 'PROCESSING' " +
                     "AND SOURCE_TYPE = 'TABLE_SAPDATA'";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.executeUpdate();
        }
    }
}
