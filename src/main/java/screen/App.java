package screen;
import java.util.Random;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

// NOTE:
// reference: https://www.javatips.net/blog/h2-in-memory-database-example

public class App {

    private static final String DB_DRIVER = "org.h2.Driver";
    private static final String DB_CONNECTION = "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1";
    private static final String DB_USER = "";
    private static final String DB_PASSWORD = "";

    public static void main(String[] args) throws Exception {
        try {
            calculate();

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void calculate() throws SQLException {

        int n = 100000;
        int number_of_group = n / 1000;
        Random r = new Random();
        double[] number = new double[n];
        for(int i = 0; i < n; i++){
            number[i] = r.nextGaussian();
        }


        Connection connection = getDBConnection();
        PreparedStatement createPreparedStatement = null;
        PreparedStatement insertPreparedStatement = null;
        PreparedStatement selectPreparedStatement = null;

        String CreateQuery = "CREATE TABLE Data(id int primary key, groupID int,NUM DOUBLE )";
        String InsertQuery = "INSERT INTO Data" + "(id, groupID, NUM) values" + "(?,?,?)";


        //String SelectQuery = "select avg(mean) from (select AVG(NUM) AS mean from Data group by groupID)";

        String SelectQuery = "select AVG(NUM) AS mean from Data group by groupID";

            connection.setAutoCommit(false);

            try {

            //create table1
            createPreparedStatement = connection.prepareStatement(CreateQuery);
            createPreparedStatement.executeUpdate();
            createPreparedStatement.close();

            // insert
            insertPreparedStatement = connection.prepareStatement(InsertQuery); //precompile
                int group = 1,count=1;
            for(int i = 0; i < n; i++) {
                    double num = number[i];
                    String ss = String.valueOf(num);
                    //String s1 = String.valueOf(i);
                    insertPreparedStatement.setInt(1, i + 1);
                    insertPreparedStatement.setInt(2, group);
                    insertPreparedStatement.setString(3, ss);
                    insertPreparedStatement.executeUpdate();
                    count++;
                    if(count > 1000){
                        group+=1;
                        count=1;
                    }

            }
            insertPreparedStatement.close();


            //select
            selectPreparedStatement = connection.prepareStatement(SelectQuery);
            ResultSet rs = selectPreparedStatement.executeQuery();  // the result
                double[] out = new double[number_of_group];
                int i = 0;
                double sum = 0, square_sum = 0, average=0;
                while (rs.next()) {
                    out[i] = rs.getDouble(1);
                    sum = sum + out[i];
                    i++;
                }

                average = sum/ (number_of_group);
                for(int j = 0; j < number_of_group; j++){
                    square_sum += (out[j] - average) * (out[j] - average);
                }

                double sigma = square_sum / (number_of_group - 1);
                System.out.println("standard deviation");
                System.out.println("---------");
                System.out.println(sigma);
                selectPreparedStatement.close();
                connection.commit();


        } catch (SQLException e) {
            System.out.println("Exception Message " + e.getLocalizedMessage());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            connection.close();
        }

    }

    private static Connection getDBConnection() {
        Connection dbConnection = null;
        try {
            Class.forName(DB_DRIVER);
        } catch (ClassNotFoundException e) {
            System.out.println(e.getMessage());
        }
        try {
            dbConnection = DriverManager.getConnection(DB_CONNECTION, DB_USER, DB_PASSWORD);
            return dbConnection;
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return dbConnection;
    }
}