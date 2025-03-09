package com.gs.tablasco.core;

import com.gs.tablasco.NamedTable;
import com.gs.tablasco.VerifiableTable;
import com.gs.tablasco.results.ExpectedResults;
import com.gs.tablasco.results.parser.ExpectedResultsParser;
import com.gs.tablasco.verify.HtmlFormatter;
import com.gs.tablasco.verify.MultiTableVerifier;
import com.gs.tablasco.verify.ResultTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.gs.tablasco.rebase.RebaseFileWriter.printTable;

public class Tablasco {

    private static final Logger LOGGER = LoggerFactory.getLogger(Tablasco.class);

    private final VerifierConfig verifierConfig;
    private final HtmlConfig htmlConfig;
    private final String testName;

    public Tablasco(VerifierConfig verifierConfig, HtmlConfig htmlConfig, String testName) {
        this.verifierConfig = verifierConfig;
        this.htmlConfig = htmlConfig;
        this.testName = testName;
    }

    public Map<String, VerifiableTable> loadBaseline(InputStream baselineInput)  {
        ExpectedResultsParser expectedResultsParser = new ExpectedResultsParser(expectedFile -> baselineInput, null);
        ExpectedResults expectedResults = expectedResultsParser.parse();
        return expectedResults.getTables(this.testName);
    }

    public void saveBaseline(List<NamedTable> tables, OutputStream outputStream)  {
        try (PrintWriter printWriter = new PrintWriter(new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8)))) {
            for (NamedTable table : tables)
            {
                LOGGER.info("Writing results for '{}'", table.getName());
                printTable(printWriter, this.testName, table.getName(), table.getTable(), this.verifierConfig.getColumnComparators());
            }
        }
    }

    public Map<String, ResultTable> verifyTables(List<NamedTable> expectedTables, List<NamedTable> actualTables)  {
        MultiTableVerifier multiTableVerifier = new MultiTableVerifier(verifierConfig);
        Map<String, ? extends VerifiableTable> expectedTableMap = expectedTables.stream()
                .collect(Collectors.toMap(NamedTable::getName, NamedTable::getTable));
        Map<String, ? extends VerifiableTable> actualTableMap = actualTables.stream()
                .collect(Collectors.toMap(NamedTable::getName, NamedTable::getTable));
        return multiTableVerifier.verifyTables(expectedTableMap, actualTableMap);
    }

    public void writeResults(Path outputFile, Map<String, ResultTable> results) {
        HtmlFormatter htmlFormatter = new HtmlFormatter(outputFile.toFile(), this.htmlConfig, new HashSet<>());
        htmlFormatter.appendResults(this.testName, results, null);
    }
}
