# Tablasco

## What is it?
Tablasco is a JUnit rule that adds table verification to your unit tests. The three main features of Tablasco together allow you to 
create maintainable tests that thoroughly verify software behaviour:
`
### 1. Fast and efficient table verification algorithm
Tablasco's table verification algorithm can process tens of thousands of table rows out-of-the-box, and many more with custom 
configuration. Verification can be configured to control behavior such as verifying row order, ignoring columns and applying tolerance to
floating-point values.

### 2. Human readable HTML break reports
Each test produces a color-coded HTML report showing how the tables matched and differed helping developers quickly identify problems.
Output can be configured to support large datasets by hiding matched rows and collapsing similar breaks.

### 3. Automatic baseline management
Each Tablasco test is backed by a text file containing expected results; this _baseline_ file is normally saved with the code in version 
control. If a test fails because of a change that causes and expected difference the test can be _rebased_ to automatically update
the text file with the new results. The updated baseline can then be committed with the corresponding software changes.

## Usage
There are two important classes to be familiar with to get started with Tablasco - `TableVerifier` and `VerifiableTable`.

### TableVerifier
`TableVerifier` is a [JUnit rule](http://junit.org/junit4/javadoc/4.12/org/junit/Rule.html) that needs to be defined as a rule in each 
Tablasco test class:
```
@Rule
public final TableVerifier tableVerifier = new TableVerifier()
    .withExpectedDir("src/test/resources")
    .withOutputDir("target");
```
Each test method in a JUnit test has its own instance of a rule and thus each Tablasco test method has its own instance of
`TableVerifier`. Tablasco takes advantage of this to offer a fluent configuration API that mutates the `TableVerifier` instance and 
allows configuration to cascade from the project, to the class, to the test level. To set project level configuration you are encouraged 
to wrap the code above in a factory method:
```
@Rule
public final TableVerifier tableVerifier = MyTableVerifierFactory.newTableVerifier();
```

For configuration that you want to apply at a class level set this when the field is initialised:
```
@Rule
public final TableVerifier tableVerifier = MyTableVerifierFactory.newTableVerifier().withHideMatchedRows(true);
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
[`VerifiableTable`](https://github.com/goldmansachs/tablasco/blob/master/src/main/java/com/gs/tablasco/VerifiableTable.java) is an 
interface that Tablasco understands as an abstract definition of a table. Tablasco tests will need to adapt the data they are verifying 
to an instance of `VerifiableTable` - typically these adapters are shared across tests or even across applications.

### Example Test
`TableVerifier` compares instances of `VerifiableTable` with the baseline results file; if the tables match the test passes and if they 
do not the test fails.

```
@Test
public void tableToBaselineTest()
{
    VerifiableTable table = new MyResultsVerifiableTable(getResults());
    this.tableVerifier.verify("results", table);
}
```

### Rebasing
To rebase a test or suite of tests set the system property `rebase=true` or configure the verifier directly using `.withRebase()`. As a
precaution to prevent developers accidentally leaving rebase switched on, rebasing tests always fail.
