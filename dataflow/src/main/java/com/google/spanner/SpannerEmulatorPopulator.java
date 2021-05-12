package com.google.spanner;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.NoCredentials;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Struct;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.Range;
import com.google.spanner.admin.database.v1.CreateDatabaseMetadata;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SpannerEmulatorPopulator is a test program that uses Dataflow to read and write to a Cloud Spanner
 * database. It can also be used to create an initial database for export. Invoke as follows:
 * <code>
 * mvn compile exec:java -Dexec.mainClass=com.google.spanner.SpannerEmulatorPopulator -Dexec.args="
 * --projectId=span-cloud-testing --endpoint=http://localhost:9010 --instanceId=test-instance --createDatabase=false --databaseIdPrefix=db-
 *  --databaseUniqueId=inbound --table=users10m --numRecords=100000 --numberOfImportWorkers=1
 *  --numWorkers=1 --maxNumWorkers=1 --runner=DirectRunner"
 * </code>
 *
 * The SPANNER_EMULATOR_HOST environment variable must be set to the emulator address and port
 * (default: http://localhost:9010).
 *
 * <p>The number of dataflow workers should be kept at 1 to avoid excessive transaction aborts,
 * since the emulator uses a simpler concurrency model with database wide locking.
 */
class SpannerEmulatorPopulator {
  private static final Logger LOG = LoggerFactory.getLogger(SpannerEmulatorPopulator.class);

  public interface SpannerPipelineOptions extends PipelineOptions {
    @Description("Project ID for Spanner")
    @Default.String("test-project")
    String getProjectId();
    void setProjectId(String value);

    // Cloud spanner emulator should be listening on port 9010.
    @Description("Spanner emulator endpoint")
    @Default.String("http://localhost:9010")
    String getEndpoint();

    void setEndpoint(String value);

    // 1B rows creates a 1 TB database
    // Each row is 1K bytes
    @Description("The expected number of rows")
    @Validation.Required
    @Default.Long(1000)
    Long getNumRecords();
    void setNumRecords(Long numRecords);

    @Description("Create a new database")
    @Default.Boolean(false)
    boolean isCreateDatabase();
    void setCreateDatabase(boolean createDatabase);

    @Description("Drop the newly created database")
    @Default.Boolean(false)
    boolean isDropDatabase();
    void setDropDatabase(boolean dropDatabase);

    @Description("Run import test")
    @Default.Boolean(true)
    boolean isRunImport();
    void setRunImport(boolean runImport);

    @Description("Run export test")
    @Default.Boolean(false)
    boolean isRunExport();
    void setRunExport(boolean runExport);

    @Description("Instance ID to write to in Spanner")
    @Validation.Required
    @Default.String("")
    String getInstanceId();
    void setInstanceId(String value);

    @Description("Database prefix to write to in Spanner")
    @Validation.Required
    @Default.String("testdb")
    String getDatabaseIdPrefix();
    void setDatabaseIdPrefix(String databaseIdPrefix);

    @Description("Database unique ID to write to in Spanner")
    @Validation.Required
    @Default.String("1234567890abcdef")
    String getDatabaseUniqueId();
    void setDatabaseUniqueId(String databaseUniqueId);

    @Description("Table name")
    @Validation.Required
    @Default.String("users")
    String getTable();
    void setTable(String value);

    @Description("Number of fields in the mutation table")
    @Default.Integer(10)
    Integer getNumberOfFields();
    void setNumberOfFields(Integer numberOfShards);

    @Description("Size of the field in bytes")
    @Default.Integer(100)
    Integer getFieldSize();
    void setFieldSize(Integer fieldSize);

    @Description("Number of dataflow workers for import")
    @Default.Integer(10)
    Integer getNumberOfImportWorkers();
    void setNumberOfImportWorkers(Integer numberOfImportWorkers);
  }

  private static String getDatabaseSchema(SpannerPipelineOptions options) {
    StringBuilder ddl = new StringBuilder(
        String.format("CREATE TABLE %s (  Key           INT64,", options.getTable()));
    for (int i = 0; i < options.getNumberOfFields(); i++) {
      ddl.append("  field").append(i).append(" STRING(MAX),");
    }
    ddl.append(" ) PRIMARY KEY (Key)");

    return ddl.toString();
  }

  private static Spanner getSpannerClient(SpannerPipelineOptions options) {
    SpannerOptions.Builder builder = SpannerOptions.newBuilder();
    if (!options.getEndpoint().isEmpty()) {
      builder.setEmulatorHost(options.getEndpoint());
    }
    SpannerOptions spannerOptions = builder.setProjectId(options.getProjectId()).build();

    return spannerOptions.getService();
  }

  private static String getDatabaseId(SpannerPipelineOptions options) {
    final int cloudSpannerNameLengthLimit = 30;
    int uniqueIdLengthLimit = cloudSpannerNameLengthLimit - options.getDatabaseIdPrefix().length();
    return options.getDatabaseIdPrefix()
        + (options.getDatabaseUniqueId().length() > uniqueIdLengthLimit
            ? options.getDatabaseUniqueId().substring(0, uniqueIdLengthLimit)
            : options.getDatabaseUniqueId());
  }

  private static void createDatabase(SpannerPipelineOptions options)
      throws ExecutionException, InterruptedException {
    LOG.info("Creating database.");
    Spanner client = getSpannerClient(options);
    DatabaseAdminClient databaseAdminClient = client.getDatabaseAdminClient();
    try {
      databaseAdminClient.dropDatabase(options.getInstanceId(), getDatabaseId(options));
    } catch (SpannerException e) {
      // Does not exist, ignore.
    }

    String ddl = getDatabaseSchema(options);
    OperationFuture<Database, CreateDatabaseMetadata> op = databaseAdminClient
        .createDatabase(options.getInstanceId(), getDatabaseId(options),
            Collections.singleton(ddl));
    op.get();
    client.close();
  }

  private static void dropDatabase(SpannerPipelineOptions options) {
    Spanner client = getSpannerClient(options);
    DatabaseAdminClient databaseAdminClient = client.getDatabaseAdminClient();
    try {
      databaseAdminClient.dropDatabase(options.getInstanceId(), getDatabaseId(options));
    } catch (SpannerException e) {
      // Does not exist, ignore.
    }
    client.close();
  }

  private static class GenerateMutations extends DoFn<Integer, Mutation> {
    private final String table;
    private final long numMutations;
    private final int numFields;
    private final int size;
    private final int numShards;
    private final Random random;

    public GenerateMutations(
        String table, long numMutations, int numFields, int size, int numShards) {
      this.table = table;
      this.numMutations = numMutations;
      this.numFields = numFields;
      this.size = size;
      this.numShards = numShards;
      this.random = new Random();
    }

    private static final char[] ALPHANUMERIC = "1234567890abcdefghijklmnopqrstuvwxyz".toCharArray();

    public String randomAlphaNumeric(int length) {
      char[] result = new char[length];
      for (int i = 0; i < length; i++) {
        result[i] = ALPHANUMERIC[random.nextInt(ALPHANUMERIC.length)];
      }
      return new String(result);
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      Integer shard = c.element();
      long numElements = numMutations / numShards;
      long start = shard * numElements;

      // The last shard extends to numMutations, to avoid generating too few rows when numElements
      // is truncated:
      long end = shard == numShards - 1 ? numMutations : start + numElements;

      for (long k = start; k < end; k++) {
        Mutation.WriteBuilder builder = Mutation.newInsertOrUpdateBuilder(table);
        builder.set("Key").to(k);
        for (int field = 0; field < numFields; field++) {
          builder.set("Field" + field).to(randomAlphaNumeric(size));
        }
        Mutation mutation = builder.build();
        c.output(mutation);
      }
    }
  }

  private static SpannerConfig getSpannerConfig(SpannerPipelineOptions options) {
    SpannerConfig config = SpannerConfig.create();
    if (!options.getEndpoint().isEmpty()) {
      config = config.withEmulatorHost(StaticValueProvider.of(options.getEndpoint()));
    }
    return config.withProjectId(options.getProjectId()).withInstanceId(options.getInstanceId())
        .withDatabaseId(getDatabaseId(options));
  }

  private static void runImport(SpannerPipelineOptions options)
      throws ExecutionException, InterruptedException {
    LOG.info("Running Import Test.");
    if (options.isCreateDatabase()) {
      createDatabase(options);
    }

    Integer numShards = options.getNumberOfImportWorkers();
    Pipeline p = Pipeline.create(options);
    PCollection<Integer> shards =
        p.apply(
            Create.of(
                ContiguousSet.create(
                    Range.closedOpen(0, numShards),
                    DiscreteDomain.integers())));

    PCollection<Mutation> mutations =
        shards.apply(
            ParDo.of(
                new GenerateMutations(
                    options.getTable(),
                    options.getNumRecords(),
                    options.getNumberOfFields(),
                    options.getFieldSize(),
                    numShards)));

    mutations.apply(SpannerIO.write().withSpannerConfig(getSpannerConfig(options)));
    p.run().waitUntilFinish();
  }

  private static void runExport(SpannerPipelineOptions options) {
    LOG.info("Running Export Test.");
    Pipeline p = Pipeline.create(options);
    PCollection<Struct> rows = p.apply(SpannerIO.read()
        .withSpannerConfig(getSpannerConfig(options))
        .withQuery("SELECT * FROM " + options.getTable()));

    PCollection<Long> count = rows.apply("Count", Count.globally());

    PAssert.thatSingleton(count).isEqualTo(options.getNumRecords());

    p.run().waitUntilFinish();

    if (options.isDropDatabase()) {
      dropDatabase(options);
    }
  }

  public static void main(String[] args) throws ExecutionException, InterruptedException {

    SpannerPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
        .as(SpannerPipelineOptions.class);

    if (options.isRunImport()) {
      runImport(options);
    }

    if (options.isRunExport()) {
      runExport(options);
    }
    LOG.info("Finished running SpannerEmulatorPopulator.");
  }
}
