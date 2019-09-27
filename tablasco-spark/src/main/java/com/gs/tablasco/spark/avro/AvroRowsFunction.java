package com.gs.tablasco.spark.avro;

import org.apache.avro.generic.GenericData;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

class AvroRowsFunction implements Function<Tuple2<AvroWrapper, NullWritable>, List<Object>>
{
    private final List<String> headers;

    AvroRowsFunction(List<String> headers)
    {
        this.headers = headers;
    }

    @Override
    public List<Object> call(Tuple2<AvroWrapper, NullWritable> avroTuple)
    {
        final GenericData.Record datum = (GenericData.Record) avroTuple._1().datum();
        List<Object> row = new ArrayList<>(this.headers.size());
        for (String header : this.headers)
        {
            Object value = datum.get(header);
            if (value instanceof CharSequence) // Avro Utf8 type
            {
                value = value.toString();
            }
            row.add(value);
        }
        return row;
    }
}
