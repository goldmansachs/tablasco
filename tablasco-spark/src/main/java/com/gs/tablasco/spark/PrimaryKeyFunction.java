package com.gs.tablasco.spark;

import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

class PrimaryKeyFunction implements PairFunction<List<Object>, List<Object>, List<Object>>
{
    private final List<String> columns;
    private final Set<String> groupKeyColumns;

    PrimaryKeyFunction(List<String> columns, Set<String> groupKeyColumns)
    {
        this.columns = columns;
        this.groupKeyColumns = groupKeyColumns;
    }

    @Override
    public Tuple2<List<Object>, List<Object>> call(List<Object> data)
    {
        List<Object> row = new ArrayList<>(this.columns.size());
        List<Object> primaryKey = new ArrayList<>(this.groupKeyColumns.size());
        for (int i = 0; i < this.columns.size(); i++)
        {
            Object value = data.get(i);
            if (this.groupKeyColumns.contains(this.columns.get(i)))
            {
                primaryKey.add(value);
            }
            row.add(value);
        }
        return new Tuple2<>(primaryKey, row);
    }
}
