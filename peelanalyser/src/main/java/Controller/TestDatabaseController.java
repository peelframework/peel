package Controller;

import java.lang.System;
import java.sql.*;

import Model.*;
import org.h2.Driver;

/**
 * Created by ubuntu on 20.10.14.
 */
public class TestDatabaseController {
    /*public static void main(String[] args) throws SQLException {
        Connection connection = null;
        try {
            Class.forName("org.h2.Driver");
            String dataBaseURL = "jdbc:h2:./src/main/resources/test";
            connection = DriverManager.getConnection(dataBaseURL);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        String queryCreateTable = "Create Table Test(Alter varchar(25));";
        String queryInsert = "Insert Into Test values('Fabian');";
        String queryInsert1 = "Insert Into Test values('Steffi');";
        String querySelect = "Select * From Test";
        String dropAll = "drop table test";
        if (connection != null) {
            Statement statement = connection.createStatement();
            statement.execute(dropAll);
            statement.execute(queryCreateTable);
            statement.execute(queryInsert);
            statement.execute(queryInsert1);
            ResultSet resultSet = statement.executeQuery(querySelect);
            resultSet.next();
            String result = resultSet.getString(1);
            System.out.println(result);
            resultSet.next();
            System.out.println(resultSet.getString(1));
        }
        connection.close();

    }
    */
}
