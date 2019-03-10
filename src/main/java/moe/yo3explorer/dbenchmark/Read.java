package moe.yo3explorer.dbenchmark;

import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Random;

public class Read implements Runnable {

    public Read(Connection connection, Random random, String name, int upperLimit) throws SQLException {
        this.connection = connection;
        this.random = random;
        this.logger = Logger.getLogger(String.format("%s%s",name,getClass().getName()));
        this.upperLimit = upperLimit;

        statement = connection.prepareStatement("SELECT * FROM dbenchmark WHERE pk=?");
    }

    private int upperLimit;
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
            while (!stopped)
            {
                statement.setInt(1,random.nextInt(upperLimit));
                ResultSet resultSet = statement.executeQuery();
                boolean hasRows = resultSet.next();
                    resultSet.getInt(1);
                    resultSet.getInt(2);
                    resultSet.getInt(3);
                    resultSet.getInt(4);
                    resultSet.getInt(5);
                    resultSet.getInt(6);
                    resultSet.getInt(7);
                setPerSecond(perSecond + 1);
                total++;
            }
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
}
