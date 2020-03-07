package com.gs.tablasco.spark.avro;

import com.gs.tablasco.spark.DistributedTable;
import org.apache.avro.mapred.AvroInputFormat;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.function.Supplier;

public class AvroDataSupplier implements Supplier<DistributedTable>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AvroDataSupplier.class);
    private final JavaSparkContext sparkContext;
    private final Path dataPath;

    public AvroDataSupplier(JavaSparkContext sparkContext, Path dataPath)
    {
        this.sparkContext = sparkContext;
        this.dataPath = dataPath;
    }

    @Override
    public DistributedTable get()
    {
        JavaPairRDD<AvroWrapper, NullWritable> avroRdd = this.sparkContext.hadoopFile(this.dataPath.toString(), AvroInputFormat.class, AvroWrapper.class, NullWritable.class);
        LOGGER.info("data location: {0}", this.dataPath);
        List<String> headers = avroRdd.keys().map(new AvroHeadersFunction()).first();
        LOGGER.info("data headers: {0}", headers);
        JavaRDD<List<Object>> rows = avroRdd.map(new AvroRowsFunction(headers));
        return new DistributedTable(headers, rows);
    }
}