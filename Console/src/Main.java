import java.sql.*;
import java.util.Scanner;

public class Main{
    // Function to establish connection
    public static Connection connectDB() {
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(
                    "jdbc:postgresql://10.100.71.21:5432/202201254",
                    "202201254",
                    "202201254"
            );
            Statement statement = connection.createStatement();
            statement.execute("SET search_path TO military_database");
            System.out.println("Connected to database 202201254");
        } catch (SQLException e) {
            System.out.println("Unable to connect!\n" + e.getMessage());
        }
        return connection;
    }

    // Function to execute a query
    public static ResultSet executeQuery(Connection connection, String query) throws SQLException {
        Statement statement = connection.createStatement();
        return statement.executeQuery(query);
    }

    // Function to execute an update query
    public static void executeUpdate(Connection connection, String query) throws SQLException {
        Statement statement = connection.createStatement();
        int rowsAffected = statement.executeUpdate(query);
        System.out.println(rowsAffected + " rows affected.");
    }

    // Function to display query results
    public static void displayResults(ResultSet resultSet) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        while (resultSet.next()) {
            for (int i = 1; i <= columnCount; i++) {
                System.out.print(resultSet.getString(i) + "\t");
            }
            System.out.println();
        }
    }

    // Menu-driven interface
    public static void menu() {
        Connection connection = connectDB();
        Scanner scanner = new Scanner(System.in);

        while (true) {
            System.out.println("\nChoose an action:");
            System.out.println("1. Update the database");
            System.out.println("2. Run a data retrieval query");
            System.out.println("3. Quit");

            String choice = scanner.nextLine();

            if (choice.equals("1")) {
                System.out.println("Enter your update query:");
                String updateQuery = scanner.nextLine();
                try {
                    executeUpdate(connection, updateQuery);
                } catch (SQLException e) {
                    System.out.println("Error executing update query: " + e.getMessage());
                }
            } else if (choice.equals("2")) {
                runDataRetrievalQuery(connection);
            } else if (choice.equals("3")) {
                break;
            } else {
                System.out.println("Invalid choice. Please enter a valid option.");
            }
        }

        try {
            connection.close();
            System.out.println("Connection closed.");
        } catch (SQLException e) {
            System.out.println("Error while closing connection: " + e.getMessage());
        }
    }

    // Function to run data retrieval query
    public static void runDataRetrievalQuery(Connection connection) {
        Scanner scanner = new Scanner(System.in);

        while (true) {
            System.out.println("\nChoose a data retrieval query to execute:");
            System.out.println("1. List all the personnel with their pension");
            System.out.println("2. Check availability of specified equipment and logistics vehicles");
            System.out.println("3.List all strategic locations in a specific war and compute its status.");
            System.out.println("0. Quit");

            String choice = scanner.nextLine();

            if (choice.equals("0")) {
                break;
            } else if (choice.equals("1")) {
                q1(connection);
            } else if (choice.equals("2")) {
                q2(connection); // Implement this method
            }
            else if(choice.equals("3")){
                q3(connection); // implement this method
            }  else {
                System.out.println("Invalid choice. Please enter a valid option.");
            }
        }
    }

    // Query 1: List all the personnel with their pension
    public static void q1(Connection connection) {
        String query = "SELECT p.ID, p.Firstname || ' ' || p.Middlename || ' ' || p.Lastname AS Fullname, " +
                "p.Gender, p.DateofBirth, p.Rank, p.ServiceStatus, p.Martyred, w.Salary, " +
                "COUNT(d.DependentName) AS NumDependents, " +
                "CASE WHEN p.Martyred = 'yes' THEN " +
                "CASE WHEN COUNT(d.DependentName) = 2 THEN (w.Salary) * (w.rate + 3)/100 " +
                "WHEN COUNT(d.DependentName) = 3 THEN (w.Salary) * (w.rate + 3.5)/100 " +
                "WHEN COUNT(d.DependentName) >= 4 THEN (w.Salary) * (w.rate + 4)/100 " +
                "ELSE w.Salary * w.rate/100 END " +
                "ELSE w.Salary * w.rate/100 END AS Pension " +
                "FROM Personnel p " +
                "INNER JOIN Work w ON p.ServiceName = w.ServiceName " +
                "LEFT JOIN Dependents d ON p.ID = d.PersonnelID " +
                "GROUP BY p.ID, p.Firstname, p.Middlename, p.Lastname, p.Gender, p.DateofBirth, " +
                "p.Rank, p.ServiceStatus, p.Martyred, w.Salary, w.rate;";
        try {
            ResultSet resultSet = executeQuery(connection, query);
            displayResults(resultSet);
        } catch (SQLException e) {
            System.out.println("Error executing query: " + e.getMessage());
        }
    }

    // Query 2: Check availability of specified equipment and logistics vehicles
    public static void q2(Connection connection) {
        String query = "WITH EquipmentAvailability AS (" +
                "SELECT" +
                " e.Name AS Equipment_Name," +
                " e.Quantity AS Total_Quantity," +
                " COALESCE(SUM(m.Quantity), 0) AS Quantity_In_Maintenance" +
                " FROM" +
                " Equipment AS e" +
                " LEFT JOIN" +
                " Maintenance AS m ON e.EquipID = m.EquipID" +
                " WHERE" +
                " e.Name IN ('M4 Carbine', 'M9 Pistol', 'M249 SAW', 'MQ-9 Reaper Drone', 'PP-19 Bizon', 'RIM-174 Standard Missile', 'MP40')" +
                " GROUP BY" +
                " e.Name, e.Quantity" +
                ")," +
                "LogisticsVehiclesAvailability AS (" +
                "SELECT" +
                " Name AS Vehicle_Name," +
                " SUM(CASE WHEN Availability = 'Available' THEN 1 ELSE 0 END) AS Total_Available" +
                " FROM" +
                " LogisticsVehicles" +
                " WHERE" +
                " Name IN ('Boeing C-17', 'Ashok Leyland FAT 6×6', 'INS Vikramaditya')" +
                " GROUP BY" +
                " Name" +
                ")" +
                "SELECT" +
                " Equipment_Name," +
                " CASE" +
                " WHEN (Total_Quantity - Quantity_In_Maintenance) >=" +
                " CASE" +
                " WHEN Equipment_Name = 'M4 Carbine' THEN 67" +
                " WHEN Equipment_Name = 'M9 Pistol' THEN 78" +
                " WHEN Equipment_Name = 'M249 SAW' THEN 10" +
                " WHEN Equipment_Name = 'MQ-9 Reaper Drone' THEN 89" +
                " WHEN Equipment_Name = 'PP-19 Bizon' THEN 56" +
                " WHEN Equipment_Name = 'RIM-174 Standard Missile' THEN 140" +
                " WHEN Equipment_Name = 'MP40' THEN 36" +
                " ELSE 0" +
                " END THEN 'Available'" +
                " ELSE 'Not Available'" +
                " END AS Equipment_Availability," +
                " NULL AS Vehicle_Name," +
                " NULL AS Vehicle_Availability" +
                " FROM" +
                " EquipmentAvailability" +
                " UNION ALL" +
                " SELECT" +
                " NULL AS Equipment_Name," +
                " NULL AS Equipment_Availability," +
                " Vehicle_Name," +
                " CASE" +
                " WHEN (CASE" +
                " WHEN Vehicle_Name = 'Boeing C-17' THEN 3" +
                " WHEN Vehicle_Name = 'Ashok Leyland FAT 6×6' THEN 2" +
                " WHEN Vehicle_Name = 'INS Vikramaditya' THEN 2" +
                " ELSE 0" +
                " END - Total_Available) >= 0 THEN 'Available'" +
                " ELSE 'Not Available'" +
                " END AS Vehicle_Availability" +
                " FROM" +
                " LogisticsVehiclesAvailability;";

        try {
            ResultSet resultSet = executeQuery(connection, query);
            displayResults(resultSet);
        } catch (SQLException e) {
            System.out.println("Error executing query: " + e.getMessage());
        }
    }

    // Query 3: List all strategic locations in a specific war and compute its status i.e. whether they are engaged or lost

    public static void q3(Connection connection){

        String query = "SELECT" +
                " SL.Location," +
                " W.Region AS War_Region," +
                " CASE" +
                " WHEN SUM(CASE WHEN P.Status = 'engaged' THEN 1 ELSE 0 END) > 0 THEN 'engaged'" +
                " WHEN COUNT(*) = SUM(CASE WHEN P.Status IN ('missing', 'POW', 'martyred') THEN 1 ELSE 0 END) THEN 'lost'" +
                " ELSE 'unknown'" +
                " END AS Status" +
                " FROM" +
                " StrategicLocation SL" +
                " LEFT JOIN pers_is_inawar P ON SL.Location = P.Location" +
                " LEFT JOIN War W ON SL.Region = W.Region" +
                " WHERE" +
                " W.Region = 'Jammu and Kashmir'" +
                " GROUP BY" +
                " SL.Location, W.Region;";

        try {
            ResultSet resultSet = executeQuery(connection, query);
            displayResults(resultSet);
        } catch (SQLException e) {
            System.out.println("Error executing query: " + e.getMessage());
        }
    }


    public static void main(String[] args) {
        menu();
    }
}