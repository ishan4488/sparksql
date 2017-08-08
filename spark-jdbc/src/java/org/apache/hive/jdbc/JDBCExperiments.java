package org.apache.hive.jdbc;


import java.sql.*;


public class JDBCExperiments {


    public static void main(String[] args) throws SQLException {

        String url = "jdbc:hive2://localhost:10000/public";
        String username = "hive";
        String password = "";

        System.out.println("-------- Spark JDBC Connection Testing ------------");

        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
        } catch (ClassNotFoundException e) {
            System.out.println("Where is your Spark JDBC Driver?");
            e.printStackTrace();
            return;
        }

        Connection connection = null;
        try {
            connection = DriverManager.getConnection(url, username, password);
        } catch (SQLException e) {
            System.out.println("Connection Failed! Check output console");
            e.printStackTrace();
            return;
        }

        if (connection != null) {
            System.out.println(" *** Connected :) ***");
        } else {
            System.out.println(" *** Connection Failed :( !!! ! ***");
        }

        System.out.println("-------------- Simple Query Get Databases -------------------------");
        Statement stmt = null;
        ResultSet res = null;
        try {
            stmt = connection.createStatement();
            res = stmt.executeQuery("show databases");
            while (res.next()) {
              System.out.println(res.getString(1));
            }
        } catch (SQLException e ) {
            printSQLException(e);
        } finally {
            if (stmt != null) {
                stmt.close();
            }
        }

        ResultSet rs = null;
        System.out.println("-------------- getMetaData() -------------------------");
        DatabaseMetaData md = connection.getMetaData();
        //ResultSet schema = metadata.getSchemas();
        //ResultSet schema = metadata.getCatalogs();
        try {
            rs = md.getColumns(null, "public", "dim_acs", null);
            while (rs.next()) {
                System.out.println("DATA_TYPE " + rs.getInt("DATA_TYPE"));
                System.out.println("COLUMN_NAME " + rs.getString("COLUMN_NAME"));
                System.out.println("TYPE_NAME " + rs.getString("TYPE_NAME"));
                System.out.println("COLUMN_SIZE " + rs.getInt("COLUMN_SIZE"));
                System.out.println("DECIMAL_DIGITS " + rs.getInt("DECIMAL_DIGITS"));
            }
        } catch (Exception e) {
            System.out.println(e);
        } finally {
            try {
                rs.close();
            } catch (Exception e) {
                // ignore
            }
        }

    }

    public static void printSQLException(SQLException ex) {

        for (Throwable e : ex) {
            if (e instanceof SQLException) {
                if (ignoreSQLException(
                        ((SQLException)e).
                                getSQLState()) == false) {

                    e.printStackTrace(System.err);
                    System.err.println("SQLState: " +
                            ((SQLException)e).getSQLState());

                    System.err.println("Error Code: " +
                            ((SQLException)e).getErrorCode());

                    System.err.println("Message: " + e.getMessage());

                    Throwable t = ex.getCause();
                    while(t != null) {
                        System.out.println("Cause: " + t);
                        t = t.getCause();
                    }
                }
            }
        }
    }

    public static boolean ignoreSQLException(String sqlState) {

        if (sqlState == null) {
            System.out.println("The SQL state is not defined!");
            return false;
        }

        // X0Y32: Jar file already exists in schema
        if (sqlState.equalsIgnoreCase("X0Y32"))
            return true;

        // 42Y55: Table already exists in schema
        if (sqlState.equalsIgnoreCase("42Y55"))
            return true;

        return false;
    }

}
