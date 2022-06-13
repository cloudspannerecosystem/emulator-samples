package com.google.cloud;

import static org.junit.Assert.*;

import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.testing.SpannerEmulator;
import com.google.common.collect.ImmutableList;
import static com.google.common.truth.Truth.assertThat;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * Simple Unit test to demonstrate the use of the {@link SpannerEmulator} {@link ClassRule} to
 * setup and teardown an emulator instance for unit testing.
 */
public class SampleEmulatorTest {
  @ClassRule
  public static SpannerEmulator emulator = SpannerEmulator.builder()
      .emulatorPort(29011)
      .emulatorRestPort(29012)
      .ddlStatements(
          "CREATE TABLE table1 (key INT64, value STRING(MAX)) PRIMARY KEY(key)",
          "CREATE TABLE table2 (key INT64, value STRING(MAX)) PRIMARY KEY(key)")
     .build();

  /**
   * Before each test, insert some data into the tables
   */
  @Before
  public void setUp() {
    emulator.getDatabaseClient().writeAtLeastOnce(ImmutableList.of(
        Mutation.newInsertBuilder("table1").set("key").to(1).set("value").to("hello world").build(),
        Mutation.newInsertBuilder("table2").set("key").to(11).set("value").to("goodbye world").build()
    ));
  }

  /**
   * After each test, clear all rows from the tables.
   */
  @After
  public void tearDown()  {
    emulator
        .getDatabaseClient()
        .readWriteTransaction()
        .run(
            txn -> {
              txn.executeUpdate(Statement.of("DELETE FROM table1 WHERE TRUE"));
              txn.executeUpdate(Statement.of("DELETE FROM table2 WHERE TRUE"));
              return null;
            });
  }

  @Test
  public void testUpdatingValues() {

    Struct row = emulator.getDatabaseClient().singleUse().readRow("table1", Key.of(1), ImmutableList.of("value"));
    assertThat(row.getString("value")).isEqualTo("hello world");

    emulator.getDatabaseClient().writeAtLeastOnce(ImmutableList.of(
        Mutation.newUpdateBuilder("table1").set("key").to(1).set("value").to("Hello Jupiter").build()));

    row = emulator.getDatabaseClient().singleUse().readRow("table1", Key.of(1), ImmutableList.of("value"));
    assertThat(row.getString("value")).isEqualTo("Hello Jupiter");
  }
}