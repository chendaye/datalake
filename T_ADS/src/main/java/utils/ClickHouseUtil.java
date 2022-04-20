package utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ClickHouseUtil {
    private static Connection connection;

    /**
     * 连接clickhouse
     * @param host
     * @param port
     * @param database
     * @return
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    public static Connection getConnection(String host, int port, String database) throws SQLException, ClassNotFoundException {
        Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
//        Class.forName("com.clickhouse.ClickHouseDriver");
        String address = "jdbc:clickhouse://" + host + ":" + port + "/" + database;
        connection = DriverManager.getConnection(address);
        return connection;
    }

    public static Connection getConnection(String host, int port) throws SQLException,ClassNotFoundException{
        return getConnection(host, port, "default");
    }

    public void close()throws SQLException {
        connection.close();
    }
}
