# Tablasco

## What is it?
Tablasco is a JUnit rule that adds table verification to your unit tests. The three main features of Tablasco combine 
to create tests that are both comprehensive and easy to maintain.

### 1. Fast and efficient table verification algorithm
Tablasco's table verification algorithm can process tens of thousands of table rows out-of-the-box, and many more with
custom configuration. Verification can be configured to control behavior such as verifying row order, ignoring headers
and applying tolerance to floating-point values.

### 2. Human readable HTML break reports
Each test produces a color-coded HTML report showing how the actual and expected tables differed helping developers 
quickly identify problems. Output can be configured to support large datasets by hiding matched rows and collapsing 
similar breaks.

![Example HTML report](img/html_report.png?raw=true)

### 3. Automatic baseline management
Each Tablasco test is backed by a text file containing expected results; this _baseline_ file is normally saved with the
code in version control. If a test fails because of a change that caused an expected difference the test can be
_rebased_ to update the text file with the new results.

## Usage
The two most important Tablasco classes to be familiar with are `TableVerifier` and `VerifiableTable`.

### TableVerifier
`TableVerifier` is a [JUnit rule](http://junit.org/junit4/javadoc/4.12/org/junit/Rule.html) that needs to be defined in
each Tablasco test class:
```
@Rule
public final TableVerifier tableVerifier = new TableVerifier()
    .withExpectedDir("src/test/resources")
    .withOutputDir("target");
```
JUnit, by design, ensures that each test method has its own instance of `TableVerifier` and Tablasco takes advantage of
this to offer a fluent API that allows configuration to cascade from the project, to the class, to the test level. To
set project level configuration you can wrap the initialisation code above in a factory method:
```
@Rule
public final TableVerifier tableVerifier = 
    MyTableVerifierFactory.newTableVerifier();
```

Configuration that you want to apply at a class level can be set when the field is initialised:
```
@Rule
public final TableVerifier tableVerifier = 
    MyTableVerifierFactory.newTableVerifier().withHideMatchedRows(true);
```

And finally, configuration required at the test level can be applied inline:
```
@Test
public void myTest()
{
    this.tableVerifier.withTolerance(0.1d).verify(tables);
}
```

### VerifiableTable
The [`VerifiableTable`](https://github.com/goldmansachs/tablasco/blob/master/src/main/java/com/gs/tablasco/VerifiableTable.java)
interface is an abstract definition of a table that Tablasco understands. Tests need to adapt the data they are verifying 
to an instance of `VerifiableTable`. Typically these adapters are shared across tests or even across applications.

### Example Test
`TableVerifier` compares instances of `VerifiableTable` with the baseline results file; if the tables match the test
passes, otherwise the test fails.

```
@Test
public void tableToBaselineTest()
{
    VerifiableTable table = new MyResultsVerifiableTable(getResults());
    this.tableVerifier.verify("results", table);
}
```

### Rebasing
To rebase a test or suite of tests set the system property `rebase=true` or configure the verifier directly using
`.withRebase()`. As a precaution to prevent developers accidentally leaving rebase switched on, rebasing tests always
fail.
