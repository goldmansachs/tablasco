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

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class IndexMapGenerator<T> {
    private final Iterator<T> actualIterator;
    private final Iterator<T> expectedIterator;
    private List<IndexMap> matched;
    private List<UnmatchedIndexMap> missing;
    private List<UnmatchedIndexMap> surplus;
    private final int initialIndex;

    IndexMapGenerator(Iterator<T> expectedIterator, Iterator<T> actualIterator, int initialIndex) {
        this.actualIterator = actualIterator;
        this.expectedIterator = expectedIterator;
        this.initialIndex = initialIndex;
    }

    public List<IndexMap> getMatched() {
        return this.matched;
    }

    public List<UnmatchedIndexMap> getMissing() {
        return this.missing;
    }

    public List<UnmatchedIndexMap> getSurplus() {
        return this.surplus;
    }

    List<IndexMap> getAll() {
        Set<IndexMap> all = new TreeSet<>();
        all.addAll(this.matched);
        all.addAll(this.surplus);
        all.addAll(this.missing);
        return new ArrayList<>(all);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    void generate() {
        this.matched = new ArrayList<>();
        this.missing = new ArrayList<>();
        this.surplus = new ArrayList<>();

        Map<T, Object> actualIndices = new LinkedHashMap<>();
        int ai = this.initialIndex;

        while (this.actualIterator.hasNext()) {
            T next = this.actualIterator.next();
            Object indexOrListOf = actualIndices.get(next);
            if (indexOrListOf == null) {
                actualIndices.put(next, ai);
            } else if (indexOrListOf instanceof Integer) {
                actualIndices.put(next, Stream.of((Integer) indexOrListOf, ai).collect(Collectors.toList()));
            } else {
                ((List) indexOrListOf).add(ai);
            }
            ai++;
        }
        int ei = this.initialIndex;
        while (this.expectedIterator.hasNext()) {
            T next = this.expectedIterator.next();
            Object actualIndexOrListOf = actualIndices.remove(next);
            if (actualIndexOrListOf == null) {
                this.missing.add(new UnmatchedIndexMap(ei, -1));
            } else if (actualIndexOrListOf instanceof Integer) {
                this.matched.add(new IndexMap(ei, (Integer) actualIndexOrListOf));
            } else {
                List indices = (List) actualIndexOrListOf;
                Integer actualIndex = (Integer) indices.remove(0);
                this.matched.add(new IndexMap(ei, actualIndex));
                actualIndices.put(next, indices.size() == 1 ? indices.get(0) : indices);
            }
            ei++;
        }
        for (Map.Entry<T, Object> actualEntry : actualIndices.entrySet()) {
            Object indexOrListOf = actualEntry.getValue();
            if (indexOrListOf instanceof Integer) {
                this.surplus.add(new UnmatchedIndexMap(-1, (Integer) indexOrListOf));
            } else {
                for (Object index : (List) indexOrListOf) {
                    this.surplus.add(new UnmatchedIndexMap(-1, (Integer) index));
                }
            }
        }
    }
}
