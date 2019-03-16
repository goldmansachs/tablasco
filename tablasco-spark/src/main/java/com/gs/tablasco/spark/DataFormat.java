package com.gs.tablasco.spark;

import org.apache.hadoop.fs.Path;

import java.util.Set;

public interface DataFormat
{
    DistributedTable getDistributedTable(Path dataLocation, Set<String> groupKeyColumns, int maximumNumberOfGroups);
}
