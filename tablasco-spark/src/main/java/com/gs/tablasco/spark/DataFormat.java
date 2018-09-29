package com.gs.tablasco.spark;

import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.eclipse.collections.api.tuple.Pair;

import java.util.List;
import java.util.Set;

public interface DataFormat
{
    Pair<List<String>, JavaPairRDD<Integer, Iterable<List<Object>>>> readHeadersAndGroups(Path dataLocation, Set<String> groupKeyColumns, int maximumNumberOfGroups);
}
