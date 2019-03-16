package com.gs.tablasco.spark.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.spark.api.java.function.Function;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

class AvroColumnsFunction implements Function<AvroWrapper, List<String>>
{
    private final Set<String> groupKeyColumns;

    AvroColumnsFunction(Set<String> groupKeyColumns)
    {
        this.groupKeyColumns = groupKeyColumns;
    }

    @Override
    public List<String> call(AvroWrapper avroWrapper)
    {
        return getColumns(((GenericData.Record) avroWrapper.datum()).getSchema().getFields());
    }

    List<String> getColumns(List<Schema.Field> fields)
    {
        List<String> columns = new ArrayList<>(fields.size());
        for (Schema.Field field : fields)
        {
            switch (field.schema().getType())
            {
                case RECORD:
                case MAP:
                case ARRAY:
                    break;
                default:
                    if (!groupKeyColumns.contains(field.name()))
                    {
                        columns.add(field.name());
                    }
                    else
                    {
                        columns.add(0, field.name());
                    }
            }
        }
        Set<String> invalidGroupKeyColumns = new HashSet<>(this.groupKeyColumns);
        invalidGroupKeyColumns.removeAll(columns);
        if (!invalidGroupKeyColumns.isEmpty())
        {
            throw new IllegalArgumentException("Invalid group key columns: " + invalidGroupKeyColumns);
        }
        return columns;
    }
}
