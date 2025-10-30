package com.thetradedesk.confetti.utils

/**
 * Guards Confetti's runtime against classpath scenarios where the Hadoop-provided
 * Guava wins ahead of the dependency bundled with the job. Jinjava 2.7.x relies on
 * {@code ImmutableMap.toImmutableMap}, which only exists in Guava 28.0 or newer;
 * if that collector is missing, the job will fail with a {@link NoSuchMethodError}
 * during template initialization.
 */
object GuavaCompatibility {

  private val ImmutableMapClass = "com.google.common.collect.ImmutableMap"

  /**
   * Ensure the loaded Guava version exposes {@code ImmutableMap.toImmutableMap}.
   * When the method is missing we surface a descriptive exception so the
   * underlying cause is clear to operators running on EMR.
   */
  def requireImmutableMapCollector(): Unit = {
    val immutableMapClass = Class.forName(ImmutableMapClass)
    val hasCollector = immutableMapClass
      .getMethods
      .exists(m => m.getName == "toImmutableMap" && m.getParameterCount == 2)

    if (!hasCollector) {
      val detectedVersion = Option(immutableMapClass.getPackage).flatMap(pkg => Option(pkg.getImplementationVersion))
      val versionSuffix = detectedVersion.map(v => s" (detected version: $v)").getOrElse("")
      throw new IllegalStateException(
        "Guava on the application classpath does not expose ImmutableMap.toImmutableMap(Function, Function)" +
          versionSuffix +
          ". Confetti's manual configuration rendering requires a Guava release built for Java 8 (e.g. 32.1.2-jre). " +
          "Package the job with a compatible Guava or override the EMR classpath so that version wins."
      )
    }
  }
}

