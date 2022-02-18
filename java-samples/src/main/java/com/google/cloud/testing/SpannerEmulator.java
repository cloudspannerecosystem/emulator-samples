package com.google.cloud.testing;

import com.google.auto.value.AutoValue;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.InstanceConfig;
import com.google.cloud.spanner.InstanceId;
import com.google.cloud.spanner.InstanceInfo;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.rules.ExternalResource;

/**
 * Junit ClassRule to create and shut down a Cloud Spanner Emulator with a Spanner database for a
 * unit test.
 *
 * <p>This rule should be used as a static {@link org.junit.ClassRule} which will be run before the
 * JUnit test class is instantiated, and after all tests are complete.
 *
 * <pre>{@code
 * @ClassRule
 * public static SpannerEmulator emulator = SpannerEmulator.builder()
 *     .projectId(PROJECT_ID)
 *     .instanceId(INSTANCE_ID)
 *     .databaseId(DATABASE_ID)
 *     .emulatorPort(EMULATOR_PORT)
 *     .emulatorRestPort(EMULATOR_REST_PORT)
 *     .ddlStatements(
 *         "CREATE TABLE table1 (key INT64, value STRING(MAX)) PRIMARY KEY(key)",
 *         "CREATE TABLE table2 (key INT64, value STRING(MAX)) PRIMARY KEY(key)")
 *     .build()
 *
 * }</pre>
 *
 * <p>Default values are available, so an empty database with no schema can be build using:
 *
 * <pre>{@code
 * @ClassRule
 * public static SpannerEmulator emulator = SpannerEmulator.builder().build()
 *
 * }</pre>
 *
 * <p>However to avoid port number collisions, it is recommended to specify port numbers for {@link
 * Builder#emulatorPort(int)} and {@link Builder#emulatorRestPort(int)}
 *
 * <p>A {@link Spanner} instance can be returned using {@link SpannerEmulator#getSpanner()}, and a
 * {@link DatabaseClient} using {@link SpannerEmulator#getDatabaseClient()}, so during a test,
 * values can be read from or written to the database using {@code emulator.getDatabaseClient()}
 *
 * <p>Requires <a
 * href="https://github.com/google/auto/blob/master/value/userguide/index.md">AutoValue</a> and <a
 * href="https://github.com/google/guava">Guava</a> to be part of your build environment.
 */
@AutoValue
public abstract class SpannerEmulator extends ExternalResource {

  private static final int DEFAULT_EMULATOR_PORT = 29010;
  private static final int DEFAULT_EMULATOR_REST_PORT = 29020;

  static {
    // This Test Rule uses Linux kill command, so only works on Unix based OS.
    if (!System.getProperty("os.name").toLowerCase().contains("linux")) {
      throw new RuntimeException(
          "SpannerEmulator TestRule is" + " only supported on Linux operating systems");
    }
  }

  // AutoValue fields
  public abstract int emulatorPort();

  public abstract int emulatorRestPort();

  public abstract String projectId();

  public abstract String instanceId();

  public abstract String databaseId();

  public abstract ImmutableList<String> ddlStatements();

  // Other fields
  private Process emulatorProcess = null;
  private Spanner spanner = null;
  private DatabaseClient databaseClient = null;

  /**
   * Builder class for {@link SpannerEmulator}.
   */
  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder emulatorPort(int emulatorPort);

    public abstract Builder emulatorRestPort(int emulatorPort);

    public abstract Builder projectId(String projectId);

    public abstract Builder instanceId(String instanceId);

    public abstract Builder databaseId(String databaseId);

    public abstract Builder ddlStatements(List<String> ddlStatements);

    public abstract Builder ddlStatements(String... ddlStatements);

    public abstract SpannerEmulator build();
  }

  /**
   * Create a Builder for a SpannerEmulator.
   */
  public static Builder builder() {
    return new AutoValue_SpannerEmulator.Builder()
        .emulatorPort(DEFAULT_EMULATOR_PORT)
        .emulatorRestPort(DEFAULT_EMULATOR_REST_PORT)
        .projectId("test")
        .instanceId("test")
        .databaseId("test")
        .ddlStatements(ImmutableList.of());
  }

  public String getEmulatorHost() {
    return "localhost:" + emulatorPort();
  }

  public DatabaseClient getDatabaseClient() {
    return databaseClient;
  }

  public Spanner getSpanner() {
    return spanner;
  }

  @Override
  protected void before() throws Throwable {

    if (emulatorProcess != null || databaseClient != null || spanner != null) {
      throw new IllegalStateException(
          "SpannerEmulator.before() called when emulator is already running");
    }

    // Just in case, kill any emulator sub processes
    killEmulatorProcesses();

    emulatorProcess =
        new ProcessBuilder()
            .inheritIO()
            .command(
                "gcloud",
                "emulators",
                "spanner",
                "start",
                "--host-port=" + getEmulatorHost(),
                "--rest-port=" + emulatorRestPort())
            .start();
    // check for startup failure
    if (emulatorProcess.waitFor(5, TimeUnit.SECONDS)) {
      emulatorProcess = null;
      throw new IllegalStateException("Emulator failed to start");
    }
    System.err.println("Spanner Emulator started on " + getEmulatorHost());

    spanner =
        SpannerOptions.newBuilder()
            .setEmulatorHost(getEmulatorHost())
            .setProjectId(projectId())
            .build()
            .getService();
    InstanceConfig config =
        spanner.getInstanceAdminClient().listInstanceConfigs().iterateAll().iterator().next();

    InstanceId instanceId = InstanceId.of(projectId(), instanceId());
    System.err.println("Creating emulated Spanner instance: " + instanceId);
    spanner
        .getInstanceAdminClient()
        .createInstance(
            InstanceInfo.newBuilder(instanceId)
                .setInstanceConfigId(config.getId())
                .setNodeCount(1)
                .build())
        .get();

    System.err.println("Creating database: " + DatabaseId.of(instanceId, databaseId()));
    Database db =
        spanner
            .getDatabaseAdminClient()
            .createDatabase(instanceId(), databaseId(), ddlStatements())
            .get();
    databaseClient = spanner.getDatabaseClient(db.getId());
    System.err.println("Emulator ready.");
  }

  @Override
  protected void after() {
    if (emulatorProcess == null || databaseClient == null || spanner == null) {
      throw new IllegalStateException("before() called when emulator is already running");
    }

    databaseClient = null;
    spanner.close();
    spanner = null;

    try {
      if (emulatorProcess.isAlive()) {
        System.err.println("Stopping Spanner Emulator");
        emulatorProcess.destroy();
        if (!emulatorProcess.waitFor(5, TimeUnit.SECONDS)) {
          emulatorProcess.destroyForcibly();
        }
        if (!emulatorProcess.waitFor(5, TimeUnit.SECONDS)) {
          throw new IllegalStateException("Emulator could not be killed");
        }
      }
    } catch (InterruptedException e) {
      // ignore and rely on killing the subprocesses to clean up
    }
    killEmulatorProcesses();
    emulatorProcess = null;
    System.err.println("Emulator stopped");
  }

  private void killEmulatorProcesses() {
    try {
      // Cleanup any leftover emulator processes
      String[] command = {
        "/bin/bash",
        "-c",
        "processes=$(/bin/ps -xo pid,command "
            + "| /bin/grep -E 'spanner_emulator/(gateway|emulator)_main .*"
            + emulatorPort()
            + "' | /bin/awk '{print $1}' ) ;"
            + " test \"$processes\" && echo Killing $processes >&2 && /bin/kill $processes"
      };
      System.err.println(
          "Checking for Spanner Emulator subprocesses: executing: " + Joiner.on(" ").join(command));
      new ProcessBuilder().inheritIO().command(command).start().waitFor();
    } catch (IOException | InterruptedException e) {
      throw new IllegalStateException(
          "Failed to shut down emulator. "
              + "'spanner_emulator/(emulator_main|gateway_main)' "
              + "processes may need to be killed manually",
          e);
    }
  }
}
