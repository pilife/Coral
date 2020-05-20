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
     * Connection factory based on the "clickhouse + postgres + memsql" model.
     */
    private static final ImmutableMap<String, String> CONFIG =
            ImmutableMap.of(
                    "model",
                    Sources.of(
                            CalciteTest.class.getResource("/model-localhost.json"))
                            .file().getAbsolutePath());


    private void execute(String sql) {
        CalciteAssert.that()
                .with(CONFIG)
                .with(Lex.JAVA)
//                .with(CalciteConnectionProperty.FIXED_PLATFORM, "JDBC.clickhouse")
//                .with(CalciteConnectionProperty.FIXED_PLATFORM, "JDBC.memsql")
//                .with(CalciteConnectionProperty.FIXED_PLATFORM, "JDBC.postgresql")
//                .with(CalciteConnectionProperty.JOIN_THRESHOLD, 4)
                .query(sql)
                // todo [coral] [P12] use .withHook()
                .returns((Consumer<ResultSet>) this::output);
    }

    private static boolean enabled() {
        return true;
    }

    @BeforeClass
    public static void setUp() {
        // run tests only if explicitly enabled
        assumeTrue("test explicitly disabled", enabled());
        // run history
    }

    @Test
    public void tpch_Q1_IN_CLICKHOUSE() {
        execute("SELECT n.n_name, s.s_suppkey, l.l_orderkey FROM postgres.nation as n " +
                "join memsql.supplier as s on s.s_nationkey = n.n_nationkey " +
                "join clickhouse.lineitem as l on l.l_suppkey = s.s_suppkey " +
                "where n.n_nationkey = 5 "
                + "and s.s_suppkey < 400 ");
    }

    @Test
    public void tpch_Q2_IN_CLICKHOUSE() {
        execute("SELECT p_partkey, s_suppkey, ps_supplycost FROM clickhouse.partsupp as ps " +
                "join memsql.supplier as s on s.s_suppkey  = ps.ps_suppkey " +
                "join memsql.part as p on p.p_partkey = ps.ps_partkey " +
                "where p.p_partkey < 400 "
                + "and s.s_suppkey < 400 ");
    }

    @Test
    public void tpch_Q3_IN_CLICKHOUSE() {
        execute("SELECT n.n_name, s.s_suppkey, ps.ps_comment FROM postgres.nation as n " +
                "join memsql.supplier as s on s.s_nationkey = n.n_nationkey " +
                "join clickhouse.partsupp as ps on ps.ps_suppkey = s.s_suppkey " +
                "where n.n_nationkey = 5 "
                + "and ps.ps_suppkey < 200 ");
    }

    @Test
    public void tpch_Q4_IN_MEMSQL() {
        execute("SELECT c_custkey, c_nationkey, o_orderkey, p_partkey, p_name FROM postgres.customer as c " +
                "join clickhouse.orders as o on o.o_custkey = c.c_custkey " +
                "join clickhouse.lineitem as l on l.l_orderkey = o.o_orderkey " +
                "join memsql.part as p on p.p_partkey = l.l_partkey " +
                "where c.c_nationkey <= 3 "
                + "and o.o_orderkey < 500 "
                + "and l.l_orderkey < 500 ");
    }

    @Test
    public void tpch_Q5_IN_MEMSQL() { // clickhouse
        execute("SELECT c_custkey, c_nationkey, o_orderkey, s_suppkey FROM postgres.customer as c " +
                "join clickhouse.orders as o on o.o_custkey = c.c_custkey " +
                "join clickhouse.lineitem as l on l.l_orderkey = o.o_orderkey " +
                "join memsql.supplier as s on s.s_suppkey  = l.l_suppkey " +
                "where c.c_nationkey <= 3 "
                + "and o.o_orderkey < 500 "
                + "and l.l_orderkey < 500 ");
    }

    @Test
    public void tpch_Q6_IN_MEMSQL() {
        execute("SELECT c_custkey, c_nationkey, o_orderkey, p_partkey, p_name FROM postgres.customer as c " +
                "join clickhouse.orders as o on o.o_custkey = c.c_custkey " +
                "join clickhouse.lineitem as l on l.l_orderkey = o.o_orderkey " +
                "join memsql.part as p on p.p_partkey = l.l_partkey " +
                "where c.c_nationkey = 5 "
                + "and o.o_orderkey < 500 "
                + "and l.l_orderkey < 500 ");
    }

    @Test
    public void tpch_Q7_IN_POSTGRESQL() {
        execute("SELECT c_custkey, c_nationkey, o_orderkey, p_partkey, p_name FROM postgres.customer as c " +
                "join clickhouse.orders as o on o.o_custkey = c.c_custkey " +
                "join clickhouse.lineitem as l on l.l_orderkey = o.o_orderkey " +
                "join memsql.part as p on p.p_partkey = l.l_partkey " +
                "where o.o_orderkey < 500 "
                + "and l.l_orderkey < 500 "
                + "and p.p_partkey < 10000 ");
    }

    @Test
    public void tpch_Q8_IN_POSTGRESQL() {
        execute("SELECT c_custkey, c_nationkey, o_orderkey, s_suppkey FROM postgres.customer as c " +
                "join clickhouse.orders as o on o.o_custkey = c.c_custkey " +
                "join clickhouse.lineitem as l on l.l_orderkey = o.o_orderkey " +
                "join memsql.supplier as s on s.s_suppkey  = l.l_suppkey " +
                "where o.o_orderkey < 500 "
                + "and l.l_orderkey < 500 "
                + "and s.s_suppkey < 10000 ");
    }

    @Test
    public void tpch_Q9_IN_POSTGRESQL() {
        execute("SELECT o_custkey, c_name, c_nationkey, p_name FROM postgres.customer as c " +
                "join clickhouse.orders as o on o.o_custkey = c.c_custkey " +
                "join clickhouse.lineitem as l on l.l_orderkey = o.o_orderkey " +
                "join memsql.part as p on p.p_partkey = l.l_partkey " +
                "where o.o_orderkey < 500 "
                + "and l.l_orderkey < 500 "
                + "and p.p_size = 1 ");
    }

    @Test
    public void tpch_Q10_IN_POSTGRESQL_CLICKHOUSE() {
        execute("SELECT c_custkey, c_nationkey, n_name, o_orderkey FROM postgres.customer as c " +
                "join clickhouse.orders as o on o.o_custkey = c.c_custkey " +
                "join postgres.nation as n on n.n_nationkey = c.c_nationkey " +
                "where n.n_nationkey = 1 limit 10 ");
    }

    @Test
    public void tpch_Q11_IN_MEMSQL_CLICKHOUSE() {
        execute("SELECT n.n_name, s.s_suppkey, ps.ps_comment FROM postgres.nation as n " +
                "join memsql.supplier as s on s.s_nationkey = n.n_nationkey " +
                "join clickhouse.partsupp as ps on ps.ps_suppkey = s.s_suppkey " +
                "where n.n_nationkey = 5 "
                + "and ps.ps_suppkey < 200 ");
    }

    @Test
    public void tpch_Q12_IN_POSTGRE_CLICKHOUSE_MEMSQL() {
        execute("SELECT c_custkey, c_nationkey, o_orderkey, s_suppkey FROM postgres.customer as c " +
                "join postgres.nation as n on n.n_nationkey = c.c_nationkey " +
                "join postgres.region as r on r.r_regionkey = n.n_regionkey " +
                "join clickhouse.orders as o on o.o_custkey = c.c_custkey " +
                "join clickhouse.lineitem as l on l.l_orderkey = o.o_orderkey " +
//                "join clickhouse.partsupp as ps on ps.ps_partkey = l.l_partkey " +
                "join memsql.part as p on p.p_partkey = l.l_partkey " +
                "join memsql.supplier as s on s.s_suppkey  = l.l_suppkey " +
                "where c.c_nationkey = 5 "
                + "and o.o_orderkey < 500 "
                + "and l.l_orderkey < 500 "
//                + "and ps.ps_partkey < 200 "
//                + "and s.s_suppkey < 500 "
//                + "and p.p_size = 1 "
        );

    }

    public void HISTORY() {
        execute("SELECT o_custkey, c_name, c_nationkey, p_name FROM postgres.customer as c, " +
                "clickhouse.orders as o, " +
                "clickhouse.lineitem as l, " +
                "memsql.part as p " +
                "where o.o_custkey = c.c_custkey " +
                "and l.l_orderkey = o.o_orderkey " +
                "and p.p_partkey = l.l_partkey "
                + "and c.c_custkey <= 10 "
                + "and o.o_custkey <= 10 "
                + "and l.l_orderkey < 40000 ");
        execute("SELECT c_custkey, c_nationkey, o_orderkey, p_partkey, p_name FROM postgres.customer as c " +
                "join clickhouse.orders as o on o.o_custkey = c.c_custkey " +
                "join clickhouse.lineitem as l on l.l_orderkey = o.o_orderkey " +
                "join memsql.part as p on p.p_partkey = l.l_partkey " +
                "where c.c_custkey <= 10 "
                + "and p.p_partkey = 17008 ");
        execute("SELECT c_custkey, c_nationkey, o_orderkey, s_suppkey FROM postgres.customer as c " +
                "join clickhouse.orders as o on o.o_custkey = c.c_custkey " +
                "join clickhouse.lineitem as l on l.l_orderkey = o.o_orderkey " +
                "join memsql.supplier as s on s.s_suppkey  = l.l_suppkey " +
                "where o.o_orderkey < 500 "
                + "and l.l_orderkey < 500 "
                + "and s.s_suppkey < 10000 ");
        execute("SELECT n.n_name, s.s_suppkey, ps.ps_comment FROM postgres.nation as n " +
                "join memsql.supplier as s on s.s_nationkey = n.n_nationkey " +
                "join clickhouse.partsupp as ps on ps.ps_suppkey = s.s_suppkey " +
                "where n.n_nationkey = 5 "
                + "and ps.ps_suppkey < 200 ");
        execute("SELECT p_partkey, s_suppkey, ps_supplycost FROM clickhouse.partsupp as ps " +
                "join memsql.supplier as s on s.s_suppkey  = ps.ps_suppkey " +
                "join memsql.part as p on p.p_partkey = ps.ps_partkey " +
                "where p.p_partkey < 400 "
                + "and s.s_suppkey < 400 ");
        execute("SELECT n.n_name, s.s_suppkey, l.l_orderkey FROM postgres.nation as n " +
                "join memsql.supplier as s on s.s_nationkey = n.n_nationkey " +
                "join clickhouse.lineitem as l on l.l_suppkey = s.s_suppkey " +
                "where n.n_nationkey = 5 "
                + "and s.s_suppkey < 400 ");
        execute("SELECT n.n_name, s.s_suppkey, ps.ps_comment FROM postgres.nation as n " +
                "join memsql.supplier as s on s.s_nationkey = n.n_nationkey " +
                "join clickhouse.partsupp as ps on ps.ps_suppkey = s.s_suppkey " +
                "where n.n_nationkey = 5 "
                + "and ps.ps_suppkey < 200 ");
        execute("SELECT n.n_name, s.s_suppkey, ps.ps_comment FROM postgres.nation as n " +
                "join memsql.supplier as s on s.s_nationkey = n.n_nationkey " +
                "join clickhouse.partsupp as ps on ps.ps_suppkey = s.s_suppkey " +
                "where n.n_nationkey = 5 "
                + "and ps.ps_suppkey < 200 ");
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
        out.println("coral query result: ");
        for (int i = 1; i <= columnCount; i++) {
            out.print(metaData.getColumnName(i) + "\t");
        }
        out.println();
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

}