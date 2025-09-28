package com.gs.tablasco.core;

import com.gs.tablasco.NamedTable;
import com.gs.tablasco.VerifiableTable;
import com.gs.tablasco.verify.ResultTable;
import de.skuzzle.test.snapshots.Snapshot;
import de.skuzzle.test.snapshots.junit5.EnableSnapshotTests;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@EnableSnapshotTests
public class HtmlTest {

    @TempDir
    Path tempDir;

    private VerifiableTable expectedTable;
    private VerifiableTable actualTable;

    @BeforeEach
    void setup() throws IOException {
        this.expectedTable = loadTable("/html-test-expected.csv");
        this.actualTable = loadTable("/html-test-actual.csv");
    }

    @Test
    void normalResult(Snapshot snapshot) throws IOException {

        HtmlConfig htmlConfig = new HtmlConfig().withAssertionSummary(true);
        runTest(htmlConfig, expectedTable, actualTable, snapshot);
    }

    @Test
    void summaryResult(Snapshot snapshot) throws IOException {

        HtmlConfig htmlConfig = new HtmlConfig().withAssertionSummary(true).withSummarizedResults(true);
        runTest(htmlConfig, expectedTable, actualTable, snapshot);
    }

    @Test
    void hideRowsColumns(Snapshot snapshot) throws IOException {

        HtmlConfig htmlConfig = new HtmlConfig()
                .withAssertionSummary(true)
                .withHideMatchedRows(true)
                .withHideMatchedColumns(true);
        runTest(htmlConfig, expectedTable, expectedTable, snapshot);
    }

    private void runTest(
            HtmlConfig htmlConfig, VerifiableTable expectedTable, VerifiableTable actualTable, Snapshot snapshot)
            throws IOException {
        Tablasco tablasco = new Tablasco(new VerifierConfig().withTolerance(0.01), htmlConfig, "HtmlTest");
        Map<String, ResultTable> verifiedTables = tablasco.verifyTables(
                Collections.singletonList(new NamedTable("Table", expectedTable)),
                Collections.singletonList(new NamedTable("Table", actualTable)));
        Path results = this.tempDir.resolve("results.html");
        tablasco.writeResults(results, verifiedTables);
        snapshot.assertThat(Files.readString(results)).asText().matchesSnapshotText();
    }

    private static CsvTable loadTable(String resource) throws IOException {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(Objects.requireNonNull(HtmlTest.class.getResourceAsStream(resource))))) {
            CSVParser parser = CSVParser.parse(reader, CSVFormat.RFC4180.withFirstRecordAsHeader());

            return new CsvTable(parser.getHeaderNames(), parser.getRecords());
        }
    }

    private record CsvTable(List<String> headerNames, List<CSVRecord> records) implements VerifiableTable {

        @Override
        public int getRowCount() {
            return this.records.size();
        }

        @Override
        public int getColumnCount() {
            return this.headerNames.size();
        }

        @Override
        public String getColumnName(int columnIndex) {
            return this.headerNames.get(columnIndex);
        }

        @Override
        public Object getValueAt(int rowIndex, int columnIndex) {
            String string = this.records.get(rowIndex).get(columnIndex);
            try {
                return Double.parseDouble(string);
            } catch (NumberFormatException e) {
                return string;
            }
        }
    }
}
