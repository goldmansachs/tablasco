package com.gs.tablasco.spark;

import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

class GroupRowsFunction implements PairFunction<List<Object>, Integer, List<Object>>
{
    private final List<String> columns;
    private final Set<String> groupKeyColumns;
    private final int numberOfGroups;

    GroupRowsFunction(List<String> columns, Set<String> groupKeyColumns, int numberOfGroups)
    {
        this.columns = columns;
        this.groupKeyColumns = groupKeyColumns;
        this.numberOfGroups = numberOfGroups;
    }

    @Override
    public Tuple2<Integer, List<Object>> call(List<Object> data)
    {
        List<Object> row = new ArrayList<>(this.columns.size());
        int hashCode = 0;
        for (int i = 0; i < this.columns.size(); i++)
        {
            Object value = data.get(i);
            if (this.groupKeyColumns.contains(this.columns.get(i)))
            {
                hashCode = combineHashes(hashCode, value == null ? 0 : value.hashCode());
            }
            row.add(value);
        }
        return new Tuple2<>(Math.abs(hashCode) % this.numberOfGroups, row);
    }

    private int combineHashes(int hash1, int hash2)
    {
        // 'borrowed' from Mithra
        hash1 += (hash2 & 0xffff);
        hash1 = (hash1 << 16) ^ ((hash2 >>> 5) ^ hash1);
        hash1 += hash1 >>> 11;
        return hash1;
    }
}
