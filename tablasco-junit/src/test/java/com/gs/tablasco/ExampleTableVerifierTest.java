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

package com.gs.tablasco;

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class ExampleTableVerifierTest {

    @RegisterExtension
    private final TableVerifier tableVerifier = new TableVerifier().withMavenDirectoryStrategy();

    @Test
    void example() {
        List<Movie> movieRanks = Arrays.asList(
                new Movie("The Batman", "2022", 1, 8.3),
                new Movie("Deep Water", "2022", 2, 5.4),
                new Movie("X", "2022", 3, 7.4),
                new Movie("The Adam Project", "2022", 4, 6.7),
                new Movie("Turning Red", "2022", 5, 7.1),
                new Movie("Windfall", "2022", 6, 5.7));
        this.tableVerifier.verify("Most Popular Movies", new MovieTable(movieRanks));
    }

    private record MovieTable(List<Movie> rows) implements VerifiableTable {

        @Override
        public int getRowCount() {
            return this.rows.size();
        }

        @Override
        public int getColumnCount() {
            return 4;
        }

        @Override
        public String getColumnName(int columnIndex) {
            return switch (columnIndex) {
                case 0 -> "Title";
                case 1 -> "Year";
                case 2 -> "User Rank";
                default -> "IMDb Rating";
            };
        }

        @Override
        public Object getValueAt(int rowIndex, int columnIndex) {
            Movie row = this.rows.get(rowIndex);
            return switch (columnIndex) {
                case 0 -> row.title;
                case 1 -> row.year;
                case 2 -> row.rank;
                default -> row.rating;
            };
        }
    }

    private record Movie(String title, String year, int rank, double rating) {}
}
