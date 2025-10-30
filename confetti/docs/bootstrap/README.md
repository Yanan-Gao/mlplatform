# Shipping a Java 8-Compatible Guava to EMR via Bootstrap

Manual Confetti jobs rely on Jinjava's default filters, which call
`ImmutableMap.toImmutableMap`. Amazon EMR images still bundle an older
Guava that does not expose this method, so the job must supply a newer
Java 8-compatible Guava at runtime. If you prefer to keep the application
code unchanged, add a bootstrap action that replaces the system Guava
before Spark starts.

## Bootstrap script

The repository ships a ready-to-use helper script:

```bash
confetti/docs/bootstrap/install-guava-32.1.2.sh
```

Upload the script to S3 and register it as a bootstrap action when you
create or clone an EMR cluster, for example:

```bash
aws emr create-cluster \
  --name "audience-calibration" \
  --release-label emr-6.14.0 \
  --applications Name=Spark \
  --bootstrap-actions Path="s3://<bucket>/bootstrap/install-guava-32.1.2.sh" \
  --steps ...
```

The script performs the following tasks on every node:

1. Moves any preinstalled `guava-*.jar` files out of `/usr/lib/hadoop/lib`
   into a backup directory so they no longer mask the newer jar.
2. Downloads `guava-32.1.2-jre.jar` from Maven Central.
3. Places the jar into `/usr/lib/hadoop/lib` with the correct
   permissions so YARN and Spark pick it up automatically.

Because the new jar lives alongside the Hadoop libraries, Spark picks it
up without extra `--jars` arguments. No changes to `ManualConfigLoader`
or other application code are required.

## Updating the version

If you need to bump the Guava version in the future, set the
`GUAVA_VERSION` environment variable when you configure the bootstrap
action or edit the script to point at a different release. The script
backs up the previous jar, so rolling forward (or back) is safe.

## Spark configuration reminder

If your job overrides classpath ordering, keep `spark.driver.userClassPathFirst`
and `spark.executor.userClassPathFirst` set to `true` in your job
configuration so that the runtime continues to prefer the newer Guava.
