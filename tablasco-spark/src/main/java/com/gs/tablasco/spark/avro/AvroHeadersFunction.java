package com.gs.tablasco.spark.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.spark.api.java.function.Function;

import java.util.ArrayList;
import java.util.List;

class AvroHeadersFunction implements Function<AvroWrapper, List<String>>
{
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
                    columns.add(field.name());
            }
        }
        return columns;
    }
}