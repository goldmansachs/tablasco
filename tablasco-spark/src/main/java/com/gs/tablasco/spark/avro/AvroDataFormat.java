package com.gs.tablasco.spark.avro;

import com.gs.tablasco.spark.DataFormat;
import com.gs.tablasco.spark.DistributedTable;
import org.apache.avro.mapred.AvroInputFormat;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

public class AvroDataFormat implements DataFormat
{
    private static final Logger LOGGER = Logger.getLogger(AvroDataFormat.class.getSimpleName());
    private final JavaSparkContext sparkContext;

    public AvroDataFormat(JavaSparkContext sparkContext)
    {
        this.sparkContext = sparkContext;
    }

    @Override
    public DistributedTable getDistributedTable(Path dataLocation, Set<String> groupKeyColumns, int maximumNumberOfGroups)
    {
        JavaPairRDD<AvroWrapper, NullWritable> avroRdd = sparkContext.hadoopFile(dataLocation.toString(), AvroInputFormat.class, AvroWrapper.class, NullWritable.class);
        LOGGER.log(Level.INFO, "data location: {0}", dataLocation);
        List<String> headers = avroRdd.keys().map(new AvroHeadersFunction(groupKeyColumns)).first();
        LOGGER.log(Level.INFO, "data headers: {0}", headers);
        JavaRDD<List<Object>> rows = avroRdd.map(new AvroRowsFunction(headers));
        return new DistributedTable(headers, rows);
    }
}
