package com.gs.tablasco.spark;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.partial.BoundedDouble;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.IOException;
import java.util.*;

public class SparkVerifierGroupKeyTest extends AbstractSparkVerifierTest
{
    private static final Schema AVSC = new Schema.Parser().parse(
            "{\"namespace\": \"verify.avro\",\n" +
            " \"type\": \"record\",\n" +
            " \"name\": \"Row\",\n" +
            " \"fields\": [\n" +
            "     {\"name\": \"k1\", \"type\": \"int\"},\n" +
            "     {\"name\": \"v1\", \"type\": \"string\"},\n" +
            "     {\"name\": \"k2\", \"type\": \"int\"},\n" +
            "     {\"name\": \"v2\", \"type\": \"double\"}\n" +
            " ]\n" +
            '}');
    private static final Schema AVSC_MISSING_COL = new Schema.Parser().parse(
            "{\"namespace\": \"verify.avro\",\n" +
            " \"type\": \"record\",\n" +
            " \"name\": \"Row\",\n" +
            " \"fields\": [\n" +
            "     {\"name\": \"k1\", \"type\": \"int\"},\n" +
            "     {\"name\": \"v1\", \"type\": \"string\"},\n" +
            "     {\"name\": \"k2\", \"type\": \"int\"}\n" +
            " ]\n" +
            '}');

    private static final List<GenericRecord> AVRO = Arrays.asList(
            row(AVSC, 12301, "123011", 12301, 123011.1),
            row(AVSC, 12301, "123012", 12302, 123012.2),
            row(AVSC, 12301, "123013", 12303, 123012.9),
            row(AVSC, 12302, "123021", 12301, 123020.8)
    );
    private static final List<GenericRecord> AVRO_X = Arrays.asList(
            row(AVSC, 12301, "123011", 12301, 123011.1),
            row(AVSC, 12301, "123012", 12302, 123012.2),
            row(AVSC, 12301, "123013x", 12303, 123012.9),
            row(AVSC, 12302, "123022", 12302, 123021.8)
    );
    private static final List<GenericRecord> AVRO_MISS_COLUMN = Arrays.asList(
            row(AVSC_MISSING_COL, 12301, "123011", 12301),
            row(AVSC_MISSING_COL, 12301, "123012", 12302),
            row(AVSC_MISSING_COL, 12301, "123013", 12303),
            row(AVSC_MISSING_COL, 12302, "123021", 12301)
    );

    @Test
    public void runTestFail() throws IOException
    {
        runTest(AVRO, AVRO_X, false, newSparkVerifier(Arrays.asList("k2", "k1"), 2));
    }

    @Test
    public void runTestPass() throws IOException
    {
        runTest(AVRO, AVRO, true, newSparkVerifier(Collections.emptyList(), 2));
    }

    @Test
    public void runTestWithSingleShardColumn() throws IOException
    {
        runTest(AVRO, AVRO_X, false, newSparkVerifier(Collections.singletonList("k2"), 2));
    }

    @Test(expected = IllegalArgumentException.class)
    public void runTestWithInvalidShardColumn() throws IOException
    {
        runTest(AVRO, AVRO_X, false, newSparkVerifier(Collections.singletonList("foo"), 2));
    }

    @Test
    public void runManyRowTest() throws IOException
    {
        List<GenericRecord> data = new ArrayList<>();
        for (Integer i = 1230001; i <= 1230100; i++)
        {
            data.add(row(AVSC, i, i.toString(), i, i * 1.0));
        }
        runTest(data, data, true, newSparkVerifier(Collections.singletonList("k1"), 10_000));
    }

    @Test
    public void runManyRowTestWithMissingColumn() throws IOException
    {
        List<GenericRecord> actual = new ArrayList<>();
        for (Integer i = 1230001; i <= 1230100; i++)
        {
            actual.add(row(AVSC_MISSING_COL, i, i.toString(), i));
        }
        List<GenericRecord> expected = new ArrayList<>();
        for (Integer i = 1230001; i <= 1230100; i++)
        {
            expected.add(row(AVSC, i, i.toString(), i, i * 1.0));
        }
        runTest(actual, expected, false, newSparkVerifier(Collections.singletonList("k1"), 10_000));
    }

    @Test
    public void runManyRowTestWithManyBreaks() throws IOException
    {
        List<GenericRecord> actual = new ArrayList<>();
        for (Integer i = 1230001; i <= 1230100; i++)
        {
            actual.add(row(AVSC, (i&8) == 0 ? i : i + 1,
                    (i&4) == 0 ? String.valueOf(i) : String.valueOf(i + 1),
                    (i&2) == 0 ? i : i + 1,
                    (i&1) == 0 ? i * 1.0 : i * 1.0 + 1));
        }
        List<GenericRecord> expected = new ArrayList<>();
        for (Integer i = 1230001; i <= 1230100; i++)
        {
            expected.add(row(AVSC, i, i.toString(), i, i * 1.0));
        }
        runTest(actual, expected, false, newSparkVerifier(Collections.emptyList(), 10_000));
    }

    @Test
    public void runManyRowTestWithManyBreaksAndRenamedColumn() throws IOException
    {
        List<GenericRecord> actual = new ArrayList<>();
        Schema schema = new Schema.Parser().parse("{\"namespace\": \"verify.avro\",\n" + " \"type\": \"record\",\n" + " \"name\": \"Row\",\n" + " \"fields\": [\n" + "     {\"name\": \"k1\", \"type\": \"int\"},\n" + "     {\"name\": \"v1\", \"type\": \"string\"},\n" + "     {\"name\": \"k2\", \"type\": \"int\"},\n" + "     {\"name\": \"v3\", \"type\": \"double\"}\n" + " ]\n" + '}');
        for (Integer i = 1230001; i <= 1230100; i++)
        {
            GenericRecord record = row(
                    schema,
                    (i & 4) == 0 ? i : i + 1,
                    (i & 2) == 0 ? String.valueOf(i) : String.valueOf(i + 1),
                    (i & 1) == 0 ? i : i + 1,
                    i * 1.0);
            actual.add(record);
        }
        List<GenericRecord> expected = new ArrayList<>();
        for (Integer i = 1230001; i <= 1230100; i++)
        {
            expected.add(row(AVSC, i, i.toString(), i, i * 1.0));
        }
        runTest(actual, expected, false, newSparkVerifier(Collections.emptyList(), 10_000));
    }

    @Test
    public void ignoreSurplusColumns() throws IOException
    {
        runTest(AVRO, AVRO_MISS_COLUMN, true, newSparkVerifier(Collections.emptyList(), 2)
                .withIgnoreSurplusColumns(true));
    }

    @Test
    public void ignoreSurplusColumnsBug() throws IOException
    {
        runTest(AVRO, AVRO_MISS_COLUMN, true, newSparkVerifier(Collections.emptyList(), 2)
                .withIgnoreSurplusColumns(true)
                .withColumnsToIgnore(new HashSet<>(Collections.singletonList("foo"))));
    }

    @Test
    public void ignoreColumns() throws IOException
    {
        runTest(AVRO, AVRO_X, false, newSparkVerifier(Collections.emptyList(), 2)
                .withColumnsToIgnore(new HashSet<>(Arrays.asList("k2", "v2"))));
    }

    @Test
    public void withToleranceColumnName() throws IOException
    {
        Schema schema = new Schema.Parser().parse(
                "{\"namespace\": \"verify.avro\",\n" +
                " \"type\": \"record\",\n" +
                " \"name\": \"Row\",\n" +
                " \"fields\": [\n" +
                "     {\"name\": \"k1\", \"type\": \"int\"},\n" +
                "     {\"name\": \"v1\", \"type\": \"string\"},\n" +
                "     {\"name\": \"k2\", \"type\": \"int\"},\n" +
                "     {\"name\": \"v2\", \"type\": \"double\"},\n" +
                "     {\"name\": \"v3\", \"type\": \"double\"}\n" +
                " ]\n" +
                '}');
        List<GenericRecord> actual = Arrays.asList(
                row(schema, 12301, "123011", 12301, 123011.1, 123021.1),
                row(schema, 12301, "123012", 12302, 123012.2, 123022.2),
                row(schema, 12301, "123013", 12303, 123012.9, 123022.9),
                row(schema, 12302, "123021", 12301, 123020.8, 123030.8));
        List<GenericRecord> expected = Arrays.asList(
                row(schema, 12301, "123011", 12301, 123011.11, 123021.11),
                row(schema, 12301, "123012", 12302, 123012.19, 123022.19),
                row(schema, 12301, "123013", 12303, 123012.95, 123022.95),
                row(schema, 12302, "123021", 12301, 123020.75, 123030.75));
        runTest(actual, expected, false, newSparkVerifier(Collections.emptyList(), 2)
                .withTolerance("v2", 0.01));
    }

    @Test
    public void withTolerance() throws IOException
    {
        List<GenericRecord> actual = Arrays.asList(
                row(AVSC, 12301, "123011", 12301, 123011.1),
                row(AVSC, 12301, "123012", 12302, 123012.2),
                row(AVSC, 12301, "123013", 12303, 123012.9),
                row(AVSC, 12302, "123021", 12301, 123020.8)
        );
        List<GenericRecord> expected = Arrays.asList(
                row(AVSC, 12301, "123011", 12301, 123011.19),
                row(AVSC, 12301, "123012", 12302, 123012.11),
                row(AVSC, 12301, "123013", 12303, 123012.79),
                row(AVSC, 12302, "123021", 12301, 123020.91)
        );
        runTest(actual, expected, false, newSparkVerifier(Collections.emptyList(), 2)
                .withTolerance(0.1));
    }

    @Test
    public void getMaximumNumberOfGroups()
    {
        Assert.assertEquals(1, SparkVerifier.getMaximumNumberOfGroups(new BoundedDouble(100.0, 50.0, 0.0, 200.0), 10_000));
        Assert.assertEquals(5, SparkVerifier.getMaximumNumberOfGroups(new BoundedDouble(100.0, 50.0, 0.0, 200.0), 20));
        Assert.assertEquals(100, SparkVerifier.getMaximumNumberOfGroups(new BoundedDouble(100.0, 50.0, 0.0, 200.0), 1));
    }

    private SparkVerifier newSparkVerifier(List<String> groupKeyColumns, int maxGroupSize)
    {
        return SparkVerifier.newWithGroupKeyColumns(groupKeyColumns, maxGroupSize).withMetadata("meta:", "data");
    }
}