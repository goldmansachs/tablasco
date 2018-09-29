package com.gs.tablasco.spark.avro;

import org.apache.avro.Schema;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class AvroHeadersFunctionTest
{
    @Test
    public void test()
    {
        MutableList<Schema.Field> fields = FastList.newListWith(
                new Schema.Field(Schema.Type.ARRAY.getName(), Schema.createArray(Schema.create(Schema.Type.BOOLEAN)), null, null),
                new Schema.Field(Schema.Type.RECORD.getName(), Schema.createRecord(Collections.<Schema.Field>emptyList()), null, null),
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

        List<String> headers = new AvroHeadersFunction(new HashSet<String>()).getHeaders(fields);

        assertEquals(
                FastList.newListWith("boolean", "string", "int", "long", "float", "bytes", "double", "null", "fixed", "enum", "union").sortThis(),
                FastList.newList(headers).sortThis());
    }

    @Test
    public void testInvalidShardColumn()
    {
        List<Schema.Field> validColumns = FastList.newListWith(new Schema.Field("FieldA", Schema.create(Schema.Type.STRING), null, null));
        try
        {
            new AvroHeadersFunction(UnifiedSet.newSetWith("FieldA", "FieldB")).getHeaders(validColumns);
            Assert.fail();
        } catch (IllegalArgumentException e)
        {
            Assert.assertEquals("Invalid group key columns: [FieldB]", e.getMessage());
        }
    }
}