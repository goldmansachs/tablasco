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

package com.gs.tablasco.verify.indexmap;

import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.impl.list.mutable.FastList;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class IndexMapGenerator<T>
{
    private final Iterator<T> actualIterator;
    private final Iterator<T> expectedIterator;
    private MutableList<IndexMap> matched;
    private MutableList<UnmatchedIndexMap> missing;
    private MutableList<UnmatchedIndexMap> surplus;
    private final int initialIndex;

    public IndexMapGenerator(Iterator<T> expectedIterator, Iterator<T> actualIterator, int initialIndex)
    {
        this.actualIterator = actualIterator;
        this.expectedIterator = expectedIterator;
        this.initialIndex = initialIndex;
    }

    public MutableList<IndexMap> getMatched()
    {
        return this.matched;
    }

    public MutableList<UnmatchedIndexMap> getMissing()
    {
        return this.missing;
    }

    public MutableList<UnmatchedIndexMap> getSurplus()
    {
        return this.surplus;
    }

    public MutableList<IndexMap> getAll()
    {
        Set<IndexMap> all = new TreeSet<>();
        all.addAll(this.matched);
        all.addAll(this.surplus);
        all.addAll(this.missing);
        return FastList.newList(all);
    }

    public void generate()
    {
        this.matched = FastList.newList();
        this.missing = FastList.newList();
        this.surplus = FastList.newList();

        Map<T, Object> actualIndices = new LinkedHashMap<T, Object>();
        int ai = this.initialIndex;

        while (this.actualIterator.hasNext())
        {
            T next = this.actualIterator.next();
            Object indexOrListOf = actualIndices.get(next);
            if (indexOrListOf == null)
            {
                actualIndices.put(next, Integer.valueOf(ai));
            }
            else if (indexOrListOf instanceof Integer)
            {
                actualIndices.put(next, FastList.newListWith((Integer) indexOrListOf, Integer.valueOf(ai)));
            }
            else
            {
                ((List) indexOrListOf).add(Integer.valueOf(ai));
            }
            ai++;
        }
        int ei = this.initialIndex;
        while (this.expectedIterator.hasNext())
        {
            T next = this.expectedIterator.next();
            Object actualIndexOrListOf = actualIndices.remove(next);
            if (actualIndexOrListOf == null)
            {
                this.missing.add(new UnmatchedIndexMap(ei, -1));
            }
            else if (actualIndexOrListOf instanceof Integer)
            {
                this.matched.add(new IndexMap(ei, (Integer) actualIndexOrListOf));
            }
            else
            {
                List indices = (List) actualIndexOrListOf;
                Integer actualIndex = (Integer) indices.remove(0);
                this.matched.add(new IndexMap(ei, actualIndex));
                actualIndices.put(next, indices.size() == 1 ? indices.get(0) : indices);
            }
            ei++;
        }
        for (Map.Entry<T, Object> actualEntry : actualIndices.entrySet())
        {
            Object indexOrListOf = actualEntry.getValue();
            if (indexOrListOf instanceof Integer)
            {
                this.surplus.add(new UnmatchedIndexMap(-1, (Integer) indexOrListOf));
            }
            else
            {
                for (Object index : (List) indexOrListOf)
                {
                    this.surplus.add(new UnmatchedIndexMap(-1, (Integer) index));
                }
            }
        }
    }

}

