package org.apache.calcite.adapter.jdbc.util;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class SparkHandler {

    public SparkSession spark;

    /**
     * Thread-safe holder
     */
    private static class Holder {
        private static final SparkHandler INSTANCE = new SparkHandler();
    }

    /**
     * Creates a SparkHandlerImpl.
     */
    private SparkHandler() {
        SparkConf conf = new SparkConf().setAppName("coral").setMaster(GlobalInfo.getInstance().getConfigThreadLocal().get().sparkMasterNode());
        spark = SparkSession.builder().config(conf).getOrCreate();
    }

    /**
     * Creates a SparkHandlerImpl, initializing on first call. Calcite-core calls
     * this via reflection.
     */
    @SuppressWarnings("UnusedDeclaration")
    public static SparkHandler instance() {
        return Holder.INSTANCE;
    }
}
