package org.apache.accumulo.testing.performance.tests;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.testing.continuous.ContinuousIngest;
import org.apache.accumulo.testing.continuous.CreateTable;
import org.apache.accumulo.testing.performance.Environment;
import org.apache.accumulo.testing.performance.PerformanceTest;
import org.apache.accumulo.testing.performance.Report;
import org.apache.accumulo.testing.performance.SystemConfiguration;
import org.apache.hadoop.io.Text;

public class RollWALPT implements PerformanceTest {
  private final String testProps = System.getProperty("user.dir")
      + "/conf/accumulo-testing.properties";
  private final String accumuloClientProps = System.getenv().get("ACCUMULO_HOME")
      + "/conf/accumulo-client.properties";
  private String entries = "";

  @Override
  public SystemConfiguration getSystemConfig() {
    return new SystemConfiguration();
  }

  @Override
  public Report runTest(Environment env) throws Exception {

    Report.Builder reportBuilder = Report.builder();
    reportBuilder.id("rollWAL");

    entries = getEntries();

    long result = getAverage(env);
    reportBuilder.result("Finished time", result, "in nanoseconds");

    walOnce(env, reportBuilder);

    return reportBuilder.build();
  }

  private String getEntries() throws IOException {
    File file = new File(testProps);
    BufferedReader br = new BufferedReader(new FileReader(file));
    String line;
    String entries = "";
    while ((line = br.readLine()) != null) {
      if (line.contains("ingest.client.entries=")) {
        entries = line.substring(0, line.indexOf("=") + 1);
        break;
      }
    }

    return entries;
  }

  private long ingest(Environment env) throws Exception {

    // Splitting the table
    final long SPLIT_COUNT = 100;
    final long distance = Long.MAX_VALUE / SPLIT_COUNT;
    final SortedSet<Text> splits = new TreeSet<>();
    for (int i = 1; i < SPLIT_COUNT; i++) {
      splits.add(new Text(String.format("%016x", i * distance)));
    }

    CreateTable.main(new String[] {testProps, accumuloClientProps});

    // Waiting for balance
    env.getClient().instanceOperations().waitForBalance();

    // Starting ingest
    final long start = System.nanoTime();

    // Load 50K 100 byte entries
    if (!entries.isEmpty())
      ContinuousIngest.main(
          new String[] {testProps, accumuloClientProps, "-o", entries + Long.toString(50 * 1000)});
    else
      System.out
          .println("ERROR:  ContinuousIngest not called because entries parameter was not found");

    final long result = System.nanoTime() - start;

    // Dropping table
    env.getClient().tableOperations().delete("ci");
    return result;
  }

  private long getAverage(Environment env) throws Exception {
    final int REPEAT = 3;
    long totalTime = 0;
    for (int i = 0; i < REPEAT; i++) {
      totalTime += ingest(env);
    }
    return totalTime / REPEAT;
  }

  public void walOnce(Environment env, Report.Builder reportBuilder) throws Exception {
    // get time with a small WAL, which will cause many WAL roll-overs
    long avg1 = getAverage(env);

    // use a bigger WAL max size to eliminate WAL roll-overs
    // using default TSERV_WALOG_MAX_SIZE of 1G
    env.getClient().tableOperations().flush(MetadataTable.NAME, null, null, true);
    env.getClient().tableOperations().flush(RootTable.NAME, null, null, true);

    long avg2 = getAverage(env);
    reportBuilder.info("Small WAL average run time", avg1, "in nanoseconds");
    reportBuilder.info("Large WAL average run time", avg2, "in nanoseconds");

    double percent = Math.round((100. * avg1) / avg2);
    reportBuilder.info("Large log", percent, "percent");
  }

}
