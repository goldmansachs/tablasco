package com.gs.tablasco.spark.avro;

import com.gs.tablasco.spark.DataFormat;
import org.apache.avro.mapred.AvroInputFormat;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.tuple.Tuples;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

public class AvroDataFormat implements DataFormat
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AvroDataFormat.class);
    private final JavaSparkContext sparkContext;

    public AvroDataFormat(JavaSparkContext sparkContext)
    {
        this.sparkContext = sparkContext;
    }

    @Override
    public Pair<List<String>, JavaPairRDD<Integer, Iterable<List<Object>>>> readHeadersAndGroups(Path dataLocation, Set<String> groupKeyColumns, int maximumNumberOfGroups)
    {
        JavaPairRDD<AvroWrapper, NullWritable> actualAvro = sparkContext.hadoopFile(dataLocation.toString(), AvroInputFormat.class, AvroWrapper.class, NullWritable.class);
        LOGGER.info("data location: {}", dataLocation);
        List<String> actualHeaders = actualAvro.keys().map(new AvroHeadersFunction(groupKeyColumns)).first();
        LOGGER.info("data headers: {}", actualHeaders);
        JavaPairRDD<Integer, Iterable<List<Object>>> actualDataByShard = actualAvro.mapToPair(new AvroGroupKeyFunction(actualHeaders, groupKeyColumns, maximumNumberOfGroups)).groupByKey();
        return Tuples.pair(actualHeaders, actualDataByShard);
    }
}
