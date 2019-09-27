package com.gs.tablasco.spark.avro;

import org.apache.avro.Schema;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

public class AvroHeadersFunctionTest
{
    @Test
    public void test()
    {
        List<Schema.Field> fields = Arrays.asList(
                new Schema.Field(Schema.Type.ARRAY.getName(), Schema.createArray(Schema.create(Schema.Type.BOOLEAN)), null, null),
                new Schema.Field(Schema.Type.RECORD.getName(), Schema.createRecord(Collections.emptyList()), null, null),
                new Schema.Field(Schema.Type.MAP.getName(), Schema.createMap(Schema.create(Schema.Type.BOOLEAN)), null, null),
                new Schema.Field(Schema.Type.BOOLEAN.getName(), Schema.create(Schema.Type.BOOLEAN), null, null),
                new Schema.Field(Schema.Type.STRING.getName(), Schema.create(Schema.Type.STRING), null, null),
                new Schema.Field(Schema.Type.INT.getName(), Schema.create(Schema.Type.INT), null, null),
                new Schema.Field(Schema.Type.LONG.getName(), Schema.create(Schema.Type.LONG), null, null),
                new Schema.Field(Schema.Type.FLOAT.getName(), Schema.create(Schema.Type.FLOAT), null, null),
                new Schema.Field(Schema.Type.BYTES.getName(), Schema.create(Schema.Type.BYTES), null, null),
                new Schema.Field(Schema.Type.DOUBLE.getName(), Schema.create(Schema.Type.DOUBLE), null, null),
                new Schema.Field(Schema.Type.NULL.getName(), Schema.create(Schema.Type.NULL), null, null),
                new Schema.Field(Schema.Type.FIXED.getName(), Schema.createFixed("fixed", null, "fixed", 1), null, null),
                new Schema.Field(Schema.Type.ENUM.getName(), Schema.createEnum("enum", null, "namespace", Arrays.asList("Foo", "Bar")), null, null),
                new Schema.Field(Schema.Type.UNION.getName(), Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING))), null, null));

        assertEquals(Schema.Type.values().length, fields.size());

        List<String> headers = new AvroHeadersFunction(new HashSet<>()).getColumns(fields);

        assertEquals(
                Stream.of("boolean", "string", "int", "long", "float", "bytes", "double", "null", "fixed", "enum", "union").sorted().collect(Collectors.toList()),
                headers.stream().sorted().collect(Collectors.toList()));
    }

    @Test
    public void testInvalidShardColumn()
    {
        List<Schema.Field> validColumns = Collections.singletonList(new Schema.Field("FieldA", Schema.create(Schema.Type.STRING), null, null));
        try
        {
            new AvroHeadersFunction(new HashSet<>(Arrays.asList("FieldA", "FieldB"))).getColumns(validColumns);
            Assert.fail();
        } catch (IllegalArgumentException e)
        {
            Assert.assertEquals("Invalid group key columns: [FieldB]", e.getMessage());
        }
    }
}