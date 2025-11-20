import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

public class AmazonRedshift
{
    // Connection object to connect to Database
    private Connection con;
    // Redshift instance Details
    private String url  = "jdbc:redshift://redshift-cluster-1.cj1cawepygbu.us-east-1.redshift.amazonaws.com:5439/dev";
    private String uid  = "awsuser";
    private String pw   = "Prakshit123#";

    // Main method
    public static void main(String[] args) throws SQLException
    {
        AmazonRedshift q = new AmazonRedshift();
        q.connect();
        q.drop();
        q.create();
        q.insert();
        q.query1();
        q.query2();
        q.query3();
        q.close();
    }

    // Method to connect to Database
    public Connection connect() throws SQLException
    {
        System.out.println("Connecting to Redshift dev database...");
        try {
            con = DriverManager.getConnection(url, uid, pw);
            System.out.println("[INFO] Connected established successfully.");
        }
        catch (SQLException e) {
            System.out.println("[ERROR] Connection failed:");
            e.printStackTrace();
        }
        return con;
    }

    // Method to close the connection from Redshift Db
    public void close()
    {
        System.out.println("[INFO] Closing the database connection.");
        try {
            if (con != null)
                con.close();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // Method to drop the tables from dev db
    public void drop()
    {
        System.out.println("Dropping all tables...");

        String[] tables = { 
            "lineitem", "orders", "customer", "partsupp", 
            "part", "supplier", "nation", "region"
        };

        try (Statement stmt = con.createStatement()) 
        {
            for (String t : tables)
            {
                stmt.execute("DROP TABLE IF EXISTS " + t + " CASCADE;");
            }
            System.out.println("Tables dropped.");
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // Method to create tables under database dev
    public void create() throws SQLException
    {
        System.out.println("[INFO] Creating the tables...");

        try 
        {
            String ddl = Files.readString(Paths.get("ddl/tpch_create.sql"));
            try (Statement stmt = con.createStatement())
            {
                for (String s : ddl.split(";"))
                {
                    if (s.trim().length() > 5)
                        stmt.execute(s);
                }
            }
            System.out.println("Schema created.");
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Insert TPC-H data from SQL insert files.
     */
    public void insert() throws SQLException {
    System.out.println("Loading TPC-H Data using batch insert...");

    List<String> files = Arrays.asList(
        "region.sql",
        "nation.sql",
        "supplier.sql",
        "part.sql",
        "partsupp.sql",
        "customer.sql",
        "orders.sql",
        "lineitem.sql"
    );

    for (String f : files) {
        System.out.println("Loading: " + f);

        try {
            String sql = Files.readString(Paths.get("data/" + f));

            try (Statement stmt = con.createStatement()) {
                int count = 0;

                // Split into individual INSERTCommands
                String[] queries = sql.split(";");

                for (String q : queries) {
                    q = q.trim();
                    if (q.length() < 5) continue; // skip invalid

                    stmt.addBatch(q);
                    count++;

                    // Execute every 500 statements
                    if (count % 500 == 0) {
                        stmt.executeBatch();
                    }
                }

                // Execute any remaining statements
                stmt.executeBatch();
            }

            System.out.println("Loaded: " + f);
        }
        catch (Exception e) {
            System.out.println("Error loading: " + f);
            e.printStackTrace();
        }
    }

    System.out.println("All data loaded using batch insert.");
    }


    /**
     * Query #1  
     * Most recent top 10 orders from customers in America.
     */
    public ResultSet query1() throws SQLException
    {
        System.out.println("Executing Query #1...");

        String sql =
            "SELECT o.orderkey, o.totalprice, o.orderdate " +
            "FROM orders o " +
            "JOIN customer c ON o.custkey = c.custkey " +
            "JOIN nation n ON c.nationkey = n.nationkey " +
            "JOIN region r ON n.regionkey = r.regionkey " +
            "WHERE r.name = 'AMERICA' " +
            "ORDER BY o.orderdate DESC " +
            "LIMIT 10;";

        PreparedStatement ps = con.prepareStatement(sql);
        ResultSet rs = ps.executeQuery();

        System.out.println(resultSetToString(rs, 10));
        return rs;
    }

    /**
     * Query #2  
     * Customer total spent, outside Europe, urgent orders, not failed,
     * belonging to the largest market segment.
     */
    public ResultSet query2() throws SQLException
    {
        System.out.println("Executing Query #2...");

        String sql =
            "SELECT c.custkey, SUM(o.totalprice) AS total_spent " +
            "FROM orders o " +
            "JOIN customer c ON o.custkey = c.custkey " +
            "JOIN nation n ON c.nationkey = n.nationkey " +
            "JOIN region r ON n.regionkey = r.regionkey " +
            "WHERE o.orderpriority = '1-URGENT' " +
            "  AND o.orderstatus != 'F' " +
            "  AND r.name != 'EUROPE' " +
            "  AND c.mktsegment = (" +
            "       SELECT mktsegment FROM customer " +
            "       GROUP BY mktsegment " +
            "       ORDER BY COUNT(*) DESC LIMIT 1" +
            "  ) " +
            "GROUP BY c.custkey " +
            "ORDER BY total_spent DESC;";

        PreparedStatement ps = con.prepareStatement(sql);
        ResultSet rs = ps.executeQuery();

        System.out.println(resultSetToString(rs, 20));
        return rs;
    }

    /**
     * Query #3  
     * Count of lineitems ordered between 1997-04-01 and 2003-04-01,
     * grouped by order priority.
     */
    public ResultSet query3() throws SQLException
    {
        System.out.println("Executing Query #3...");

        String sql =
            "SELECT o.orderpriority, COUNT(*) AS num_items " +
            "FROM lineitem l " +
            "JOIN orders o ON l.orderkey = o.orderkey " +
            "WHERE o.orderdate >= '1997-04-01' " +
            "  AND o.orderdate <  '2003-04-01' " +
            "GROUP BY o.orderpriority " +
            "ORDER BY o.orderpriority ASC;";

        PreparedStatement ps = con.prepareStatement(sql);
        ResultSet rs = ps.executeQuery();

        System.out.println(resultSetToString(rs, 50));
        return rs;
    }

    /*************************************************************************
     * Utility: Convert ResultSet into formatted string
     *************************************************************************/
    public static String resultSetToString(ResultSet rst, int maxrows) throws SQLException
    {
        StringBuffer buf = new StringBuffer(5000);
        int rowCount = 0;
        ResultSetMetaData meta = rst.getMetaData();

        buf.append("Total columns: " + meta.getColumnCount());
        buf.append('\n');

        if (meta.getColumnCount() > 0)
            buf.append(meta.getColumnName(1));

        for (int j = 2; j <= meta.getColumnCount(); j++)
            buf.append(", " + meta.getColumnName(j));

        buf.append("\n---------------------------------------------------\n");

        while (rst.next())
        {
            if (rowCount < maxrows)
            {
                for (int j = 1; j <= meta.getColumnCount(); j++)
                {
                    Object obj = rst.getObject(j);
                    buf.append(obj);
                    if (j != meta.getColumnCount())
                        buf.append(", ");
                }
                buf.append('\n');
            }
            rowCount++;
        }
        buf.append("Total results: " + rowCount);
        return buf.toString();
    }
}
