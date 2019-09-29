package com.gs.tablasco.spark;

import org.apache.hadoop.fs.Path;

/**
 * A DataFormat instance knows how to load an HDFS dataset and convert it to the required format for verification
 */
public interface DataFormat
{
    /**
     * Loads an HDFS dataset and returns a DistributedTable
     * @param dataLocation the HDFS data location
     * @return a DistributedTable
     */
    DistributedTable getDistributedTable(Path dataLocation);
}
