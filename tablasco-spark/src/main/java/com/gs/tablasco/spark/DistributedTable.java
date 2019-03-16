package com.gs.tablasco.spark;

import org.apache.spark.api.java.JavaPairRDD;

import java.util.List;

@SuppressWarnings("WeakerAccess")
public class DistributedTable
{
    private final List<String> columns;
    private final JavaPairRDD<Integer, Iterable<List<Object>>> distributedRows;

    public DistributedTable(List<String> columns, JavaPairRDD<Integer, Iterable<List<Object>>> distributedRows)
    {
        this.columns = columns;
        this.distributedRows = distributedRows;
    }

    public List<String> getColumns()
    {
        return columns;
    }

    public JavaPairRDD<Integer, Iterable<List<Object>>> getDistributedRows()
    {
        return distributedRows;
    }
}
