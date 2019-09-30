package com.gs.tablasco.spark.avro;

import com.gs.tablasco.spark.DistributedTable;
import org.apache.avro.mapred.AvroInputFormat;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

public class AvroDataSupplier implements Supplier<DistributedTable>
{
    private static final Logger LOGGER = Logger.getLogger(AvroDataSupplier.class.getSimpleName());
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
        LOGGER.log(Level.INFO, "data location: {0}", this.dataPath);
        List<String> headers = avroRdd.keys().map(new AvroHeadersFunction()).first();
        LOGGER.log(Level.INFO, "data headers: {0}", headers);
        JavaRDD<List<Object>> rows = avroRdd.map(new AvroRowsFunction(headers));
        return new DistributedTable(headers, rows);
    }
}