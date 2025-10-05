package com.gs.tablasco.core;

import static org.junit.jupiter.api.Assertions.*;

import com.gs.tablasco.VerifiableTable;
import java.util.function.Function;
import java.util.function.Predicate;
import org.junit.jupiter.api.Test;

class VerifierConfigTest {

    @Test
    void testDefaultConfiguration() {
        VerifierConfig config = new VerifierConfig();

        assertTrue(config.isVerifyRowOrder());
        assertFalse(config.isIgnoreSurplusRows());
        assertFalse(config.isIgnoreMissingRows());
        assertFalse(config.isIgnoreSurplusColumns());
        assertFalse(config.isIgnoreMissingColumns());
        assertEquals(300000L, config.getPartialMatchTimeoutMillis()); // Default timeout
    }

    @Test
    void testRowOrderConfiguration() {
        VerifierConfig config = new VerifierConfig().withVerifyRowOrder(false);

        assertFalse(config.isVerifyRowOrder());
    }

    @Test
    void testIgnoreRowsConfiguration() {
        VerifierConfig config = new VerifierConfig().withIgnoreSurplusRows().withIgnoreMissingRows();

        assertTrue(config.isIgnoreSurplusRows());
        assertTrue(config.isIgnoreMissingRows());
    }

    @Test
    void testIgnoreColumnsConfiguration() {
        VerifierConfig config = new VerifierConfig().withIgnoreSurplusColumns().withIgnoreMissingColumns();

        assertTrue(config.isIgnoreSurplusColumns());
        assertTrue(config.isIgnoreMissingColumns());
    }

    @Test
    void testPartialMatchTimeout() {
        long timeout = 5000L;
        VerifierConfig config = new VerifierConfig().withPartialMatchTimeoutMillis(timeout);

        assertEquals(timeout, config.getPartialMatchTimeoutMillis());
    }

    @Test
    void testNoPartialMatchTimeout() {
        VerifierConfig config = new VerifierConfig().withoutPartialMatchTimeout();

        assertEquals(0L, config.getPartialMatchTimeoutMillis());
    }

    @Test
    void testColumnFilter() {
        Predicate<String> columnFilter = column -> column.startsWith("test");
        VerifierConfig config = new VerifierConfig().withColumnFilter(columnFilter);

        assertNotNull(config.getActualAdapter());
        assertNotNull(config.getExpectedAdapter());
    }

    @Test
    void testIgnoreSpecificColumns() {
        VerifierConfig config = new VerifierConfig().withIgnoreColumns("col1", "col2");

        assertNotNull(config.getActualAdapter());
        assertNotNull(config.getExpectedAdapter());
    }

    @Test
    void testToleranceConfiguration() {
        double tolerance = 0.001;
        VerifierConfig config = new VerifierConfig().withTolerance(tolerance).withTolerance("column1", tolerance);

        assertNotNull(config.getColumnComparators());
    }

    @Test
    void testVarianceThresholdConfiguration() {
        double threshold = 0.1;
        VerifierConfig config =
                new VerifierConfig().withVarianceThreshold(threshold).withVarianceThreshold("column1", threshold);

        assertNotNull(config.getColumnComparators());
    }

    @Test
    void testCustomAdapters() {
        Function<VerifiableTable, VerifiableTable> adapter = table -> table;
        VerifierConfig config = new VerifierConfig().withActualAdapter(adapter).withExpectedAdapter(adapter);

        assertSame(adapter, config.getActualAdapter());
        assertSame(adapter, config.getExpectedAdapter());
    }
}
