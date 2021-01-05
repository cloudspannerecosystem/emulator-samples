Usage:

1. Set the emulator host environment variable:
  export SPANNER_EMULATOR_HOST=localhost:9010
2. Start the emulator
  docker run -p 9010:9010 --rm -it <image>
3. Run the Spanner samples.
  java -jar java-samples-1.0-SNAPSHOT.jar <operation> <instance> <database>
