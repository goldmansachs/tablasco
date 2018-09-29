package com.gs.tablasco.spark.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.spark.api.java.function.Function;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;

import java.util.List;
import java.util.Set;

class AvroHeadersFunction implements Function<AvroWrapper, List<String>>
{
    private final Set<String> groupKeyColumns;

    AvroHeadersFunction(Set<String> groupKeyColumns)
    {
        this.groupKeyColumns = groupKeyColumns;
    }

    @Override
    public List<String> call(AvroWrapper avroWrapper)
    {
        return getHeaders(GenericData.Record.class.cast(avroWrapper.datum()).getSchema().getFields());
    }

    List<String> getHeaders(List<Schema.Field> fields)
    {
        List<String> headers = FastList.newList(fields.size());
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
                        headers.add(field.name());
                    }
                    else
                    {
                        headers.add(0, field.name());
                    }
            }
        }
        Set<String> invalidGroupKeyColumns = UnifiedSet.newSet(this.groupKeyColumns).withoutAll(headers);
        if (!invalidGroupKeyColumns.isEmpty())
        {
            throw new IllegalArgumentException("Invalid group key columns: " + invalidGroupKeyColumns);
        }
        return headers;
    }
}
