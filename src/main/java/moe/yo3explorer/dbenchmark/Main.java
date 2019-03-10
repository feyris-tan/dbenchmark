package moe.yo3explorer.dbenchmark;

import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Random;

public class Main
{
    public static void main(String[] args) throws ClassNotFoundException, SQLException, InterruptedException {
        new Main().run();
    }

    private final int benchmarkDuration = 10;
    private Logger logger;

    public void run() throws ClassNotFoundException, SQLException, InterruptedException {
        logger = Logger.getLogger(getClass());
        logger.info("Hello!");

        Class.forName("org.mariadb.jdbc.Driver");
        BenchmarkResults mariaDbResult = doBenchmark("jdbc:mariadb://127.0.0.1:3306/dbenchmark", "root", "12345","MariaDB");

        Class.forName("org.postgresql.Driver");
        BenchmarkResults postgresResult = doBenchmark("jdbc:postgresql://127.0.0.1:5432/dbenchmark", "postgres", "12345","PostgreSQL");

        logger.info("The results are in!");
        logger.info(String.format(" Reads: Postgresql: %d per second. MariaDB %d per second",postgresResult.getReadAverage(),mariaDbResult.getReadAverage()));
        logger.info(String.format("Writes: Postgresql: %d per second. MariaDB %d per second",postgresResult.getWriteAverage(),mariaDbResult.getWriteAverage()));
    }

    private BenchmarkResults doBenchmark(String url, String username, String password,String jobName) throws SQLException, InterruptedException {
        BenchmarkResults result = new BenchmarkResults();
        result.reads = new int[benchmarkDuration];
        result.writes = new int[benchmarkDuration];

        Random random = new Random();
        Connection connection = DriverManager.getConnection(url,username,password);
        Write writer = new Write(connection,random,jobName);
        Thread thread = new Thread(writer);
        thread.start();
        for (int i = 0; i < benchmarkDuration; i++)
        {
            Thread.sleep(1000);
            logger.info(String.format("%s: %d inserts/second",jobName,writer.getPerSecond()));
            result.writes[i] = writer.getPerSecond();
            writer.setPerSecond(0);
        }
        writer.setStopped(true);
        thread.join();

        Read reader = new Read(connection,random,jobName,writer.getTotal());
        thread = new Thread(reader);
        thread.start();
        for (int i = 0; i < benchmarkDuration; i++)
        {
            Thread.sleep(1000);
            logger.info(String.format("%s: %d selects/second",jobName,reader.getPerSecond()));
            result.reads[i] = reader.getPerSecond();
            reader.setPerSecond(0);
        }
        reader.setStopped(true);
        thread.join();
        return result;
    }

    private class BenchmarkResults
    {
        int[] reads;
        int[] writes;

        public int getReadAverage()
        {
            long total = 0;
            for (int i = 0; i < reads.length; i++)
            {
                total += reads[i];
            }
            return (int)(total / reads.length);
        }

        public int getWriteAverage()
        {
            long total = 0;
            for (int i = 0; i < writes.length; i++)
            {
                total += writes[i];
            }
            return (int)(total / writes.length);
        }
    }
}
