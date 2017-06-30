/*
 * Copyright 2017 Goldman Sachs.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.gs.tablasco.verify;

import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.tuple.Tuples;

import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

public class Metadata
{
    private static final Format DATE_TIME_FORMATTER = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public static final String RECORDED_AT = "Recorded At";

    private final List<Pair<String, String>> data = FastList.newList();

    private Metadata()
    {
    }

    public static Metadata newEmpty()
    {
        return new Metadata();
    }

    public static Metadata newWithRecordedAt()
    {
        Metadata metadata = new Metadata();
        metadata.addDate(RECORDED_AT, new Date());
        return metadata;
    }

    public void add(String key, String value)
    {
        this.data.add(Tuples.pair(key, value));
    }

    public void addDate(String key, Date date)
    {
        this.add(key, DATE_TIME_FORMATTER.format(date));
    }

    public List<Pair<String, String>> getData()
    {
        return this.data;
    }

    @Override
    public String toString()
    {
        return toString("");
    }

    public String toString(String stringQualifier)
    {
        StringBuilder builder = new StringBuilder();
        makeString(builder, stringQualifier);
        return builder.toString();
    }

    private void makeString(StringBuilder builder, String stringQualifier)
    {
        Iterator<Pair<String, String>> iterator = this.data.iterator();
        while (iterator.hasNext())
        {
            Pair<String, String> pair = iterator.next();
            addData(pair.getOne(), pair.getTwo(), builder, stringQualifier);
            if (iterator.hasNext())
            {
                builder.append(',').append(' ');
            }
        }
    }

    private static void addData(String key, String value, StringBuilder builder, String stringQualifier)
    {
        builder.append(stringQualifier).append(key).append(stringQualifier);
        builder.append(' ');
        builder.append(stringQualifier).append(value).append(stringQualifier);
    }
}
