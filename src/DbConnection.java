/**
 * Created by Bence on 2015.04.12..
 */

import java.sql.*;


public class DbConnection {
    //the connection name and url
    protected static final  String driverName="org.apache.hive.jdbc.HiveDriver";
    protected static final  String connectionUrl="jdbc:hive2://127.0.0.1:10000/";

    //the connection object
    protected Connection connection=null;

    public DbConnection(){}

    public void connect(String userName, String password) throws SQLException, ClassNotFoundException {

        // If connection status is disconnected
        if (connection == null || !connection.isValid(30)) {

            if (connection == null) {

                // Load the specified database driver
                Class.forName(driverName);

            } else {

                connection.close();

            }

            System.out.println("kapcsolodas");
            // Create new connection
            connection = DriverManager.getConnection(connectionUrl, userName, password);

        }

    }

}
