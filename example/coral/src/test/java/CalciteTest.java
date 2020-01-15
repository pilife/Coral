import junit.framework.TestCase;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.commons.dbcp2.BasicDataSource;

import java.sql.*;
import java.util.Properties;

public class CalciteTest extends TestCase {

    private CalciteConnection calciteConnection;

    private void initConnection() throws SQLException {
        final String UNIONSQL = "select *\n"
                + "from " + VALUES1 + "\n"
                + " union all\n"
                + "select *\n"
                + "from " + VALUES2;

        Properties info = new Properties();
        info.setProperty("lex", "JAVA");
//        info.setProperty("spark", "true");
        Connection connection =
                DriverManager.getConnection("jdbc:calcite:", info);
        calciteConnection =
                connection.unwrap(CalciteConnection.class);
        SchemaPlus rootSchema = calciteConnection.getRootSchema();

        addPostgreSchema(rootSchema);
        addHRSchema(rootSchema);
//        addSparksqlSchema(rootSchema);
        addClickhouseSchema(rootSchema);
    }


    public void testMain() throws SQLException {

        initConnection();

        Statement statement = calciteConnection.createStatement();

        ResultSet resultSet = statement.executeQuery(OLTP_CLICKHOUSE_SQL);

        output(resultSet);

        statement.close();
        calciteConnection.close();
    }

    private final String HR_SQL = "select * \n"
            + "from hr.depts as d\n";

    private final String POSTGRE_HR_SQL = "select * \n"
            + "from postgre.employees as e\n"
            + "join hr.depts as d\n"
            + "  on e.dept_no = d.deptno\n";

    private final String POSTGRE_SPARKSQL_SQL = "select * \n"
            + "from postgre.employees as e\n"
            + "join sparksql.dept as d\n"
            + "  on e.dept_no = d.deptno\n";

    private final String CLICKHOUSE_1_SQL = "SELECT AirlineID, COUNT(1) FROM clickhouse.ontime group by AirlineID";

    private final String CLICKHOUSE_POSTGRES_1_SQL = "SELECT count(*) FROM clickhouse.ontime as o join postgre.airline as a on o.AirlineID = a.id where o.AirlineID = 19707 limit 10";

    private final String OLTP_CLICKHOUSE_POSTGRES_HR_2_SQL =
            "SELECT project_name, name, deptName " +
                    "FROM clickhouse.em_projects as p " +
                    "join postgre.employees as e on p.manager_id = e.id " +
                    "join hr.depts as d on e.dept_no = d.deptno";

    private final String OLTP_CLICKHOUSE_POSTGRES_SQL =
            "SELECT project_name, name " +
                    "FROM clickhouse.em_projects as p " +
                    "join postgre.employees as e on p.manager_id = e.id";

    private final String OLTP_CLICKHOUSE_SQL =
            "SELECT project_name, project_name " +
                    "FROM clickhouse.em_projects as p";

    private final String VALUES0 = "(values (1, 'a'), (2, 'b'))";

    private final String VALUES1 =
            "(values (1, 'a'), (2, 'b')) as t(x, y)";

    private final String VALUES2 =
            "(values (1, 'a'), (2, 'b'), (1, 'b'), (2, 'c'), (2, 'c')) as t(x, y)";

    private final String VALUES3 =
            "(values (1, 'a'), (2, 'b')) as v(w, z)";

    private final String VALUES4 =
            "(values (1, 'a'), (2, 'b'), (3, 'b'), (4, 'c'), (2, 'c')) as t(x, y)";

    private final String LITERAL_UNION_SQL = "select *\n"
            + "from " + VALUES1 + "\n"
            + " union all\n"
            + "select *\n"
            + "from " + VALUES2;

    private void addPostgreSchema(SchemaPlus rootSchema) {
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setUrl("jdbc:postgresql://simple31:5432/test");
        dataSource.setUsername("zhangyi");
        dataSource.setPassword("root");
        Schema schema = JdbcSchema.create(rootSchema, "postgre", dataSource,
                null, "public");
        rootSchema.add("postgre", schema);
    }

    private void addClickhouseSchema(SchemaPlus rootSchema) {
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setUrl("jdbc:clickhouse://simple31:8123/default");
        dataSource.setUsername("default");
        dataSource.setPassword("");
        Schema schema = JdbcSchema.create(rootSchema, "clickhouse", dataSource,
                null, "default");
        rootSchema.add("clickhouse", schema);
    }

    private void addHRSchema(SchemaPlus rootSchema) {
        Schema schema = new ReflectiveSchema(new HrSchema());
        rootSchema.add("hr", schema);
    }

    private void addSparksqlSchema(SchemaPlus rootSchema) {
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setUrl("jdbc:hive2://simple31:10000");
        dataSource.setUsername("zhangyi");
        dataSource.setPassword("");
        Schema schema = JdbcSchema.create(rootSchema, "sparksql", dataSource,
                null, "public");
        rootSchema.add("sparksql", schema);
    }


    private void output(ResultSet resultSet) throws SQLException {
        ResultSetMetaData rsmd = resultSet.getMetaData();
        int columnsNumber = rsmd.getColumnCount();
        for (int i = 1; i <= columnsNumber; i++) {
            System.out.print(rsmd.getColumnName(i) + "\t");
        }
        System.out.println();
        while (resultSet.next()) {
            for (int i = 1; i <= columnsNumber; i++) {
                String columnValue = resultSet.getString(i);
                System.out.print(columnValue + "\t");
            }
            System.out.println();
        }
        resultSet.close();
    }

    @SuppressWarnings("WeakerAccess")
    public static class HrSchema {
        public final Department[] depts = new Department[4];

        HrSchema() {
            for (int i = 0; i < 4; i++) {
                Department department = new Department();
                department.deptno = i;
                department.deptName = "部门" + i;
                depts[i] = department;
            }
        }

        public static class Department {
            public int deptno;
            public String deptName;
        }
    }


}