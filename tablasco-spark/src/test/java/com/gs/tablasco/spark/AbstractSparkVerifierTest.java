package com.gs.tablasco.spark;

import com.gs.tablasco.spark.avro.AvroDataSupplier;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class AbstractSparkVerifierTest
{
    private static final boolean REBASE = false;

    static GenericRecord row(Schema schema, Object... values)
    {
        GenericRecord record = new GenericData.Record(schema);
        for (int i = 0; i < schema.getFields().size(); i++)
        {
            record.put(schema.getFields().get(i).name(), values[i]);
        }
        return record;
    }

    private static final JavaSparkContext JAVA_SPARK_CONTEXT = new JavaSparkContext("local[4]", AbstractSparkVerifierTest.class.getSimpleName(),  new SparkConf()
            .set("spark.ui.enabled", "false")
            .set("spark.logLineage", "true")
            .set("spark.sql.shuffle.partitions", "10")
            .set("spark.task.maxFailures", "1")
            .set("spark.io.compression.codec", "org.apache.spark.io.LZ4CompressionCodec"));

    @Rule
    public final TestName testName = new TestName();

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    protected void runTest(List<GenericRecord> actual, List<GenericRecord> expected, boolean passed, SparkVerifier sparkVerifier) throws IOException
    {
        File actualData = this.temporaryFolder.newFile("actual.avro");
        writeAvroData(actual, actualData);
        File expectedDate = this.temporaryFolder.newFile("expected.avro");
        writeAvroData(expected, expectedDate);
        SparkResult sparkResult = sparkVerifier.verify("data",
                new AvroDataSupplier(JAVA_SPARK_CONTEXT, new Path(actualData.toURI().toString())),
                new AvroDataSupplier(JAVA_SPARK_CONTEXT, new Path(expectedDate.toURI().toString())));
        String html = sparkResult.getHtml();

        java.nio.file.Path baselineFile = Paths.get("src", "test", "resources", this.getClass().getSimpleName(), this.testName.getMethodName() + ".html");
        if (REBASE)
        {
            baselineFile.toFile().getParentFile().mkdirs();
            Files.write(baselineFile, html.getBytes());
            Assert.fail("REBASE SUCCESSFUL - " + baselineFile);
        }
        Assert.assertEquals(passed, sparkResult.isPassed());
        Assert.assertEquals(
                replaceValuesThatMayAppearInNonDeterministicRowOrder(new String(Files.readAllBytes(baselineFile))),
                replaceValuesThatMayAppearInNonDeterministicRowOrder(html));
    }

    private static String replaceValuesThatMayAppearInNonDeterministicRowOrder(String value)
    {
        return value
                // mask all test values because output order is non-deterministic (test values start with 1230)
                .replaceAll(">1,?2,?3,?0,?\\S*?([ <])", ">###$1")
                // mask variance percentages
                .replaceAll("/ [\\d\\.-]+%", "###%");
    }

    private static void writeAvroData(List<GenericRecord> data, File avroFile) throws IOException
    {
        FileUtils.forceMkdir(avroFile.getParentFile());
        Schema schema = data.get(0).getSchema();
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.create(schema, avroFile);
        for (GenericRecord genericRecord : data)
        {
            dataFileWriter.append(genericRecord);
        }
        dataFileWriter.close();
    }
}