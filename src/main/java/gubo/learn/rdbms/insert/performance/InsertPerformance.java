package gubo.learn.rdbms.insert.performance;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.UUID;


/**
 *
 * CREATE DATABASE testdb;
 *
 * CREATE TABLE `insert_performance` (
 *   `id` int(11) NOT NULL AUTO_INCREMENT,
 *   `content` varchar(45) NOT NULL,
 *   PRIMARY KEY (`id`)
 * ) ENGINE=InnoDB AUTO_INCREMENT=40002 DEFAULT CHARSET=utf8
 *
 * */
public class InsertPerformance {

    private static HikariConfig config = new HikariConfig();
    private static HikariDataSource ds;

    static {
        config.setJdbcUrl( "jdbc:mysql://localhost:3306/testdb?characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&autoReconnect=true" );
        config.setUsername( "root" );
        config.setPassword( "" );
        config.addDataSourceProperty( "cachePrepStmts" , "true" );
        config.addDataSourceProperty( "prepStmtCacheSize" , "250" );
        config.addDataSourceProperty( "prepStmtCacheSqlLimit" , "2048" );
        ds = new HikariDataSource( config );
    }

    public static Connection getConnection() throws SQLException {
        return ds.getConnection();
    }

    public static void bench(int totalRows, int rowsPerTransaction, int concurrency ) throws SQLException {

        if (concurrency == 1) {
            doWork(totalRows, rowsPerTransaction);
        }
    }

    public static void doWork(int totalRows, int rowsPerTransaction) throws SQLException {

        Connection conn = getConnection();
        conn.setAutoCommit(false);
        int totalResidue = totalRows;
        int tranxResidue = rowsPerTransaction;

        PreparedStatement stmt = conn.prepareStatement("INSERT INTO insert_performance (content) values (?)");
        while (totalResidue > 0) {
            tranxResidue = rowsPerTransaction;
            while(totalResidue > 0 && tranxResidue > 0) {

                String content = UUID.randomUUID().toString();
                stmt.setString(1, content);
                stmt.execute();

                -- totalResidue;
                -- tranxResidue;
            }
            conn.commit();
        }


    }
    public static void main(String[] args) throws SQLException {

        int total = 10000;

        long ts1 = System.currentTimeMillis();
        bench(total, 1,1);
        long ts2 = System.currentTimeMillis();
        bench(total, 1000, 1);
        long ts3 = System.currentTimeMillis();


        System.out.println("ts2 - ts1 = " + (ts2-ts1));
        System.out.println("ts3 - ts2 = " + (ts3-ts2));

    }
}
