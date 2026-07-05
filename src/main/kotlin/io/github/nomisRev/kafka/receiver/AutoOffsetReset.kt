package io.github.nomisRev.kafka.receiver

public enum class AutoOffsetReset(public val value: String) {
  /* Option to reset to the earliest available offsets if no initial or current offsets exist for the consumer group. */
  Earliest("earliest"),

  /* Option to reset to the latest available offsets if no initial or current offsets exist for the consumer group. */
  Latest("latest"),

  /* Option to fail the consumer if there are no offsets available for the consumer group. */
  None("none")
}