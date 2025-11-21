import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class AmazonRedshift {

    /** JDBC Configuration — your actual Redshift details */
    private static final String HOST     = "redshift-cluster-1.cj1cawepygbu.us-east-1.redshift.amazonaws.com";
    private static final String PORT     = "5439";
    private static final String DB_NAME  = "dev";
    private static final String USER     = "awsuser";
    private static final String PASSWORD = "Prakshit123#";

    /** S3 Bucket & IAM Role — YOUR valid values */
    private static final String S3_BUCKET = "assignment6-ankit";
    private static final String S3_PREFIX = "";  // files are directly in root — no folder
    private static final String IAM_ROLE_ARN =
        "arn:aws:iam::411080313195:role/service-role/AmazonRedshift-CommandsAccessRole-20251121T101848";

    /** DDL file path */
    private static final String DDL_FILE = "ddl/tpch_create.sql";

    private Connection con;

    public static void main(String[] args) {
        AmazonRedshift app = new AmazonRedshift();
        try {
            app.connect();
            app.drop();
            app.create();
            app.insert();   // COPY from S3 (fast)
            app.query1();
            app.query2();
            app.query3();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            app.close();
        }
    }

    /** Connect to AWS Redshift */
    public Connection connect() throws SQLException {
        if (con != null && !con.isClosed()) return con;

        String url = "jdbc:redshift://" + HOST + ":" + PORT + "/" + DB_NAME;

        System.out.println("Connecting to Redshift: " + url);
        con = DriverManager.getConnection(url, USER, PASSWORD);

        System.out.println("Connected successfully.\n");
        return con;
    }

    /** Close connection */
    public void close() {
        System.out.println("Closing database connection.");
        try {
            if (con != null) con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /** Drop tables in dependency-safe order */
    public void drop() {
        System.out.println("Dropping TPC-H tables...");

        String[] drops = new String[] {
            "DROP TABLE IF EXISTS LINEITEM CASCADE",
            "DROP TABLE IF EXISTS ORDERS CASCADE",
            "DROP TABLE IF EXISTS CUSTOMER CASCADE",
            "DROP TABLE IF EXISTS PARTSUPP CASCADE",
            "DROP TABLE IF EXISTS PART CASCADE",
            "DROP TABLE IF EXISTS SUPPLIER CASCADE",
            "DROP TABLE IF EXISTS NATION CASCADE",
            "DROP TABLE IF EXISTS REGION CASCADE"
        };

        try (Statement stmt = con.createStatement()) {
            for (String sql : drops) {
                System.out.println("Executing: " + sql);
                stmt.execute(sql);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        System.out.println("Drop completed.\n");
    }

    /** Execute DDL from file */
    public void create() {
        System.out.println("Creating schema from DDL...");

        try {
            String ddl = new String(Files.readAllBytes(Paths.get(DDL_FILE)),
                    StandardCharsets.UTF_8);

            String[] stmts = ddl.split(";");

            try (Statement stmt = con.createStatement()) {
                for (String s : stmts) {
                    if (s.trim().length() > 0) {
                        System.out.println("Executing DDL statement...");
                        stmt.execute(s);
                    }
                }
            }

            System.out.println("Schema created.\n");

        } catch (Exception e) {
            System.out.println("Error reading/executing DDL.");
            e.printStackTrace();
        }
    }

    /** COPY all .tbl files from S3 into Redshift */
    public void insert() {
        System.out.println("Loading data using COPY from S3...\n");

        String keyPrefix = S3_PREFIX.isEmpty() ? "" : (S3_PREFIX + "/");

        String[] copyStatements = new String[] {

            "COPY REGION FROM 's3://" + S3_BUCKET + "/" + keyPrefix + "region.tbl' "
            + "IAM_ROLE '" + IAM_ROLE_ARN + "' DELIMITER '|' DATEFORMAT 'YYYY-MM-DD'",

            "COPY NATION FROM 's3://" + S3_BUCKET + "/" + keyPrefix + "nation.tbl' "
            + "IAM_ROLE '" + IAM_ROLE_ARN + "' DELIMITER '|' DATEFORMAT 'YYYY-MM-DD'",

            "COPY SUPPLIER FROM 's3://" + S3_BUCKET + "/" + keyPrefix + "supplier.tbl' "
            + "IAM_ROLE '" + IAM_ROLE_ARN + "' DELIMITER '|' DATEFORMAT 'YYYY-MM-DD'",

            "COPY PART FROM 's3://" + S3_BUCKET + "/" + keyPrefix + "part.tbl' "
            + "IAM_ROLE '" + IAM_ROLE_ARN + "' DELIMITER '|' DATEFORMAT 'YYYY-MM-DD'",

            "COPY PARTSUPP FROM 's3://" + S3_BUCKET + "/" + keyPrefix + "partsupp.tbl' "
            + "IAM_ROLE '" + IAM_ROLE_ARN + "' DELIMITER '|' DATEFORMAT 'YYYY-MM-DD'",

            "COPY CUSTOMER FROM 's3://" + S3_BUCKET + "/" + keyPrefix + "customer.tbl' "
            + "IAM_ROLE '" + IAM_ROLE_ARN + "' DELIMITER '|' DATEFORMAT 'YYYY-MM-DD'",

            "COPY ORDERS FROM 's3://" + S3_BUCKET + "/" + keyPrefix + "orders.tbl' "
            + "IAM_ROLE '" + IAM_ROLE_ARN + "' DELIMITER '|' DATEFORMAT 'YYYY-MM-DD'",

            "COPY LINEITEM FROM 's3://" + S3_BUCKET + "/" + keyPrefix + "lineitem.tbl' "
            + "IAM_ROLE '" + IAM_ROLE_ARN + "' DELIMITER '|' DATEFORMAT 'YYYY-MM-DD'"
        };

        try (Statement stmt = con.createStatement()) {
            for (String sql : copyStatements) {
                System.out.println("Running: " + sql);
                long start = System.currentTimeMillis();
                stmt.executeUpdate(sql);
                long end = System.currentTimeMillis();
                System.out.println(" → Completed in " + (end - start) / 1000.0 + " seconds\n");
            }
        } catch (SQLException e) {
            System.out.println("Error during COPY:");
            e.printStackTrace();
        }

        System.out.println("All COPY operations finished.\n");
    }

    /** QUERY #1 */
    public ResultSet query1() throws SQLException {
        System.out.println("Executing Query 1: Top 10 most recent orders by customers in AMERICA.\n");

        String sql =
            "SELECT o.O_ORDERKEY, o.O_TOTALPRICE, o.O_ORDERDATE "
          + "FROM ORDERS o "
          + "JOIN CUSTOMER c ON o.O_CUSTKEY = c.C_CUSTKEY "
          + "JOIN NATION n ON c.C_NATIONKEY = n.N_NATIONKEY "
          + "JOIN REGION r ON n.N_REGIONKEY = r.R_REGIONKEY "
          + "WHERE r.R_NAME = 'AMERICA' "
          + "ORDER BY o.O_ORDERDATE DESC "
          + "LIMIT 10";

        PreparedStatement ps = con.prepareStatement(sql);
        ResultSet rs = ps.executeQuery();
        System.out.println(resultSetToString(rs, 10));
        return rs;
    }

    /** QUERY #2 */
    public ResultSet query2() throws SQLException {
        System.out.println("Executing Query 2...\n");

        String sql =
            "SELECT c.C_CUSTKEY, SUM(o.O_TOTALPRICE) AS TOTAL_SPENT "
          + "FROM ORDERS o "
          + "JOIN CUSTOMER c ON o.O_CUSTKEY = c.C_CUSTKEY "
          + "JOIN NATION n ON c.C_NATIONKEY = n.N_NATIONKEY "
          + "JOIN REGION r ON n.N_REGIONKEY = r.R_REGIONKEY "
          + "WHERE o.O_ORDERPRIORITY = '1-URGENT' "
          + "  AND o.O_ORDERSTATUS <> 'F' "
          + "  AND r.R_NAME <> 'EUROPE' "
          + "  AND c.C_MKTSEGMENT = ( "
          + "        SELECT C_MKTSEGMENT "
          + "        FROM CUSTOMER "
          + "        GROUP BY C_MKTSEGMENT "
          + "        ORDER BY COUNT(*) DESC LIMIT 1 "
          + "  ) "
          + "GROUP BY c.C_CUSTKEY "
          + "ORDER BY TOTAL_SPENT DESC";

        PreparedStatement ps = con.prepareStatement(sql);
        ResultSet rs = ps.executeQuery();
        System.out.println(resultSetToString(rs, 50));
        return rs;
    }

    /** QUERY #3 */
    public ResultSet query3() throws SQLException {
        System.out.println("Executing Query 3...\n");

        String sql =
            "SELECT o.O_ORDERPRIORITY, COUNT(*) AS NUM_ITEMS "
          + "FROM LINEITEM l "
          + "JOIN ORDERS o ON l.L_ORDERKEY = o.O_ORDERKEY "
          + "WHERE o.O_ORDERDATE >= DATE '1997-04-01' "
          + "  AND o.O_ORDERDATE <  DATE '2003-04-01' "
          + "GROUP BY o.O_ORDERPRIORITY "
          + "ORDER BY o.O_ORDERPRIORITY";

        PreparedStatement ps = con.prepareStatement(sql);
        ResultSet rs = ps.executeQuery();
        System.out.println(resultSetToString(rs, 50));
        return rs;
    }

    /** Utility: convert ResultSet to readable output */
    public static String resultSetToString(ResultSet rst, int maxrows) throws SQLException {
        StringBuilder sb = new StringBuilder(4096);
        ResultSetMetaData meta = rst.getMetaData();
        int cols = meta.getColumnCount();

        for (int i = 1; i <= cols; i++) {
            sb.append(meta.getColumnName(i)).append(" | ");
        }
        sb.append("\n");

        int count = 0;
        while (rst.next()) {
            if (count < maxrows) {
                for (int j = 1; j <= cols; j++) {
                    sb.append(rst.getObject(j)).append(" | ");
                }
                sb.append("\n");
            }
            count++;
        }

        sb.append("Total rows: ").append(count);
        return sb.toString();
    }
}
