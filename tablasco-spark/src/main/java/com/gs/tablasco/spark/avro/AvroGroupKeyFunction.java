package com.gs.tablasco.spark.avro;

import org.apache.avro.generic.GenericData;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

class AvroGroupKeyFunction implements PairFunction<Tuple2<AvroWrapper, NullWritable>, Integer, List<Object>>
{
    private final List<String> columns;
    private final Set<String> groupKeyColumns;
    private final int numberOfGroups;

    AvroGroupKeyFunction(List<String> columns, Set<String> groupKeyColumns, int numberOfGroups)
    {
        this.columns = columns;
        this.groupKeyColumns = groupKeyColumns;
        this.numberOfGroups = numberOfGroups;
    }

    @Override
    public Tuple2<Integer, List<Object>> call(Tuple2<AvroWrapper, NullWritable> avroTuple)
    {
        final GenericData.Record datum = (GenericData.Record) avroTuple._1().datum();
        List<Object> row = new ArrayList<>(this.columns.size());
        int hashCode = 0;
        for (String header : this.columns)
        {
            Object value = datum.get(header);
            if (value instanceof CharSequence) // Avro Utf8 type
            {
                value = value.toString();
            }
            if (this.groupKeyColumns.contains(header))
            {
                hashCode = combineHashes(hashCode, value == null ? 0 : value.hashCode());
            }
            row.add(value);
            // if (!name.equals("attributes") && !name.equals("balances"))
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
