package com.gs.tablasco.spark;

import org.apache.spark.api.java.JavaRDD;

import java.util.List;

@SuppressWarnings("WeakerAccess")
public class DistributedTable
{
    private final List<String> headers;
    private final JavaRDD<List<Object>> rows;

    public DistributedTable(List<String> headers, JavaRDD<List<Object>> rows)
    {
        this.headers = headers;
        this.rows = rows;
    }

    public List<String> getHeaders()
    {
        return headers;
    }

    public JavaRDD<List<Object>> getRows()
    {
        return rows;
    }
}
