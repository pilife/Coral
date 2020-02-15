import com.google.common.collect.ImmutableMap;
import org.apache.calcite.config.Lex;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.util.Sources;
import org.apache.calcite.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.PrintStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.function.Consumer;

import static org.junit.Assume.assumeTrue;

public class CalciteTest {

    /**
     * Connection factory based on the "clickhouse + postgres" model.
     */
    private static final ImmutableMap<String, String> CONFIG =
            ImmutableMap.of(
                    "model",
                    Sources.of(
                            CalciteTest.class.getResource("/model-localhost.json"))
                            .file().getAbsolutePath());


    private static boolean enabled() {
        return true;
    }

    @BeforeClass
    public static void setUp() {
        // run tests only if explicitly enabled
        assumeTrue("test explicitly disabled", enabled());
    }


    @Test
    public void testOLTP_CLICKHOUSE_POSTGRES() {
        CalciteAssert.that()
                .with(CONFIG)
                .with(Lex.JAVA)
                .query("SELECT project_name, name FROM clickhouse.em_projects as p join postgres.employees as e on p.manager_id = e.id")
                .returns((Consumer<ResultSet>) this::output);
    }

    @Test
    public void testOLTP_CLICKHOUSE() {
        CalciteAssert.that()
                .with(CONFIG)
                .with(Lex.JAVA)
                .query("SELECT project_name, project_name FROM clickhouse.em_projects as p")
                .returns((Consumer<ResultSet>) this::output);
    }


    private Void output(ResultSet resultSet) {
        try {
            output(resultSet, System.out);
        } catch (SQLException e) {
            throw TestUtil.rethrow(e);
        }
        return null;
    }

    private void output(ResultSet resultSet, PrintStream out)
            throws SQLException {
        final ResultSetMetaData metaData = resultSet.getMetaData();
        final int columnCount = metaData.getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
            System.out.print(metaData.getColumnName(i) + "\t");
        }
        System.out.println();
        while (resultSet.next()) {
            for (int i = 1; ; i++) {
                out.print(resultSet.getString(i));
                if (i < columnCount) {
                    out.print(", ");
                } else {
                    out.println();
                    break;
                }
            }
        }
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