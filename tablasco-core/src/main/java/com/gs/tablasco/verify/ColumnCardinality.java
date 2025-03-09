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

import java.io.Serializable;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;

class ColumnCardinality implements Serializable {
    private final int maximumCardinalityToCount;
    private Map<Object, Integer> bag = new HashMap<>();

    ColumnCardinality(int maximumCardinalityToCount) {
        this.maximumCardinalityToCount = maximumCardinalityToCount;
    }

    int getDistinctCount() {
        if (this.isFull()) {
            return this.maximumCardinalityToCount;
        }
        return this.bag.size();
    }

    void merge(ColumnCardinality that) {
        if (that.isFull()) {
            this.setFull();
        } else {
            that.bag.forEach(ColumnCardinality.this::addOccurrences);
        }
    }

    void addOccurrence(Object value) {
        this.addOccurrences(value, 1);
    }

    void removeOccurrence(Object value) {
        if (!this.isFull()) {
            Integer integer = this.bag.get(value);
            if (integer != null) {
                if (integer > 1) {
                    this.bag.put(value, integer - 1);
                } else {
                    this.bag.remove(value);
                }
            }
        }
    }

    void forEachWithOccurrences(final BiConsumer<Object, Integer> procedure) {
        this.bag.entrySet().stream()
                .sorted(Comparator.comparing((Function<Map.Entry<Object, Integer>, Integer>) Map.Entry::getValue)
                        .reversed())
                .limit(this.maximumCardinalityToCount)
                .forEach(objectIntegerEntry ->
                        procedure.accept(objectIntegerEntry.getKey(), objectIntegerEntry.getValue()));
    }

    boolean isFull() {
        return this.bag == null;
    }

    private void addOccurrences(Object value, int occurrences) {
        if (!this.isFull()) {
            Integer cardinality = this.bag.get(value);
            if (cardinality != null || this.bag.size() < this.maximumCardinalityToCount) {
                this.bag.put(value, (cardinality == null ? 0 : cardinality) + occurrences);
            } else {
                this.setFull();
            }
        }
    }

    private void setFull() {
        this.bag = null;
    }
}
