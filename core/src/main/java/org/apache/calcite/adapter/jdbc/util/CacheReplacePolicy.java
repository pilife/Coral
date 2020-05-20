package org.apache.calcite.adapter.jdbc.util;

import lombok.AllArgsConstructor;

import java.util.Map;
import java.util.TreeMap;

public class CacheReplacePolicy {

    public static final int capicity = 10;

    public static Map<String, MultiLayerScore> map = new TreeMap<>();

    public static boolean isQueryNeedCached(String query){
        return true;
    }

    public static void doCacheReplace(String query){

    }

    @AllArgsConstructor
    static class MultiLayerScore{

        double historyScore;

        double costScore;

    }

}
