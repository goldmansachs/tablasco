package com.gs.tablasco.core;

import com.gs.tablasco.NamedTable;
import com.gs.tablasco.VerifiableTable;
import com.gs.tablasco.verify.*;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Tablasco {

    private final VerifierConfig verifierConfig;
    private final HtmlConfig htmlConfig;
    private final String testName;

    public Tablasco(VerifierConfig verifierConfig, HtmlConfig htmlConfig, String testName) {
        this.verifierConfig = verifierConfig;
        this.htmlConfig = htmlConfig;
        this.testName = testName;
    }

    public Map<String, ResultTable> verifyTables(List<NamedTable> expectedTables, List<NamedTable> actualTables) {
        Map<String, VerifiableTable> expectedTableMap =
                expectedTables.stream().collect(Collectors.toMap(NamedTable::getName, NamedTable::getTable));
        Map<String, VerifiableTable> actualTableMap =
                actualTables.stream().collect(Collectors.toMap(NamedTable::getName, NamedTable::getTable));
        return this.verifyTables(expectedTableMap, actualTableMap);
    }

    public Map<String, ResultTable> verifyTables(
            Map<String, VerifiableTable> expectedTables, Map<String, VerifiableTable> actualTables) {
        MultiTableVerifier multiTableVerifier = new MultiTableVerifier(verifierConfig);
        return multiTableVerifier.verifyTables(expectedTables, actualTables);
    }

    public void writeResults(Path outputFile, Map<String, ResultTable> results) {
        HtmlFormatter htmlFormatter = new HtmlFormatter(outputFile.toFile(), this.htmlConfig, new HashSet<>());
        htmlFormatter.appendResults(this.testName, results, null);
    }
}
