package moe.yo3explorer.dbenchmark;

import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Random;

public class Write implements Runnable{

    public Write(Connection connection, Random random,String name) throws SQLException {
        this.connection = connection;
        this.random = random;
        this.logger = Logger.getLogger(String.format("%s%s",name,getClass().getName()));

        try {
            statement = connection.prepareStatement("CREATE TABLE dbenchmark (pk BIGINT PRIMARY KEY, a INT, b INT, c INT, d INT, e INT, f INT)");
            statement.executeUpdate();
            statement.close();
        }
        catch (SQLException se)
        {
            logger.info("dbenchmark already exists, but that's okay.");
        }

        statement = connection.prepareStatement("TRUNCATE dbenchmark");
        statement.executeUpdate();
        statement.close();

        statement = connection.prepareStatement("INSERT INTO dbenchmark (pk,a,b,c,d,e,f) VALUES (?,?,?,?,?,?,?)");
    }

    private PreparedStatement statement;
    private Logger logger;
    private Connection connection;
    private Random random;

    private int perSecond;
    private int total;
    private boolean stopped;

    public void run() {
        stopped = false;
        try {
            connection.setAutoCommit(false);
            while (!stopped)
            {
                statement.setInt(1,total++);
                statement.setInt(2,random.nextInt());
                statement.setInt(3,random.nextInt());
                statement.setInt(4,random.nextInt());
                statement.setInt(5,random.nextInt());
                statement.setInt(6,random.nextInt());
                statement.setInt(7,random.nextInt());
                statement.executeUpdate();
                setPerSecond(perSecond + 1);
            }
            connection.commit();
            connection.setAutoCommit(true);
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public synchronized void setPerSecond(int perSecond) {
        this.perSecond = perSecond;
    }

    public void setStopped(boolean stopped) {
        this.stopped = stopped;
    }

    public int getPerSecond() {
        return perSecond;
    }

    public int getTotal() {
        return total;
    }
}
