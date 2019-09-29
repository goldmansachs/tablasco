package com.gs.tablasco.spark;

import org.apache.spark.api.java.JavaRDD;

import java.util.List;

/**
 * A distributed table that can be compared by SparkVerifier
 */
@SuppressWarnings("WeakerAccess")
public class DistributedTable
{
    private final List<String> headers;
    private final JavaRDD<List<Object>> rows;

    /**
     * @param headers table headers (column names)
     * @param rows An RDD of table rows with a row represented as a list of objects
     */
    public DistributedTable(List<String> headers, JavaRDD<List<Object>> rows)
    {
        this.headers = headers;
        this.rows = rows;
    }

    /**
     * @return the table headers (column names)
     */
    public List<String> getHeaders()
    {
        return headers;
    }

    /**
     * @return An RDD of table rows with a row represented as a list of objects
     */
    public JavaRDD<List<Object>> getRows()
    {
        return rows;
    }
}
