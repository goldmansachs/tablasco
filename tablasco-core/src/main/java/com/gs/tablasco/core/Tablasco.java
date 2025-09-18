package com.gs.tablasco.core;

import com.gs.tablasco.NamedTable;
import com.gs.tablasco.VerifiableTable;
import com.gs.tablasco.verify.HtmlFormatter;
import com.gs.tablasco.verify.MultiTableVerifier;
import com.gs.tablasco.verify.ResultTable;
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
        MultiTableVerifier multiTableVerifier = new MultiTableVerifier(verifierConfig);
        Map<String, ? extends VerifiableTable> expectedTableMap =
                expectedTables.stream().collect(Collectors.toMap(NamedTable::getName, NamedTable::getTable));
        Map<String, ? extends VerifiableTable> actualTableMap =
                actualTables.stream().collect(Collectors.toMap(NamedTable::getName, NamedTable::getTable));
        return multiTableVerifier.verifyTables(expectedTableMap, actualTableMap);
    }

    public void writeResults(Path outputFile, Map<String, ResultTable> results) {
        HtmlFormatter htmlFormatter = new HtmlFormatter(outputFile.toFile(), this.htmlConfig, new HashSet<>());
        htmlFormatter.appendResults(this.testName, results, null);
    }
}
