package io.github.nomisrev.kafka.receiver

import io.github.nomisRev.kafka.receiver.CommitStrategy
import io.github.nomisRev.kafka.receiver.ReceiverSettings
import io.github.nomisRev.kafka.receiver.internals.EventLoop
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.Job
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.MockConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.consumer.OffsetCommitCallback
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.clients.consumer.RetriableCommitFailedException
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.RebalanceInProgressException
import org.apache.kafka.common.errors.TopicAuthorizationException
import org.apache.kafka.common.serialization.StringDeserializer
import org.junit.jupiter.api.Test

/**
 * A commit that fails because it raced a group rebalance resolves on its own: once the rebalance
 * completed, the commit can simply be retried. Failing the receive flow instead turns every commit
 * that races a routine rebalance - scaling, deployments, member restarts - into a fatal error.
 *
 * These tests fail the *first* commit and then let every following one succeed, so they can tell
 * apart the two answers the event loop can give: retry, or close the channel.
 */
class RebalanceRacingCommitSpec {

  @Test
  fun `a commit racing a rebalance is retried instead of failing the receive flow`() = runBlocking {
    val consumer = failingTheFirstCommitWith(RebalanceInProgressException("commit raced the rebalance"))
    val collecting = collect(consumer)

    try {
      withTimeout(30.seconds) {
        while (consumer.successfulCommits.isEmpty()) delay(20)
      }
      assertEquals(1L, consumer.successfulCommits.first()[PARTITION]?.offset())
      assertNull(
        collecting.flowFailure.takeIf { it.isCompleted }?.await(),
        "a commit that raced a rebalance must not fail the receive flow"
      )
    } finally {
      collecting.close()
    }
  }

  @Test
  fun `a commit the broker asks to retry is still retried`() = runBlocking {
    val consumer = failingTheFirstCommitWith(RetriableCommitFailedException("try again"))
    val collecting = collect(consumer)

    try {
      withTimeout(30.seconds) {
        while (consumer.successfulCommits.isEmpty()) delay(20)
      }
      assertEquals(1L, consumer.successfulCommits.first()[PARTITION]?.offset())
      assertNull(
        collecting.flowFailure.takeIf { it.isCompleted }?.await(),
        "the retry the broker asked for must not fail the receive flow"
      )
    } finally {
      collecting.close()
    }
  }

  @Test
  fun `a commit failure that will not resolve on its own still fails the receive flow`() = runBlocking {
    val fatal = TopicAuthorizationException("not allowed to commit")
    val consumer = failingTheFirstCommitWith(fatal)
    val collecting = collect(consumer)

    try {
      val failure = withTimeout(30.seconds) { collecting.flowFailure.await() }
      /* kotlinx.coroutines copies an exception when it crosses coroutines to recover its stack
       * trace, so the collector sees an equal failure, not the very same instance. */
      assertEquals(fatal::class, failure::class)
      assertEquals(fatal.message, failure.message)
      assertEquals(
        emptyList<Map<TopicPartition, OffsetAndMetadata>>(),
        consumer.successfulCommits.toList(),
        "a failure that will not resolve on its own must not be retried"
      )
    } finally {
      collecting.close()
    }
  }
}

private const val TOPIC = "commit-race-topic"
private val PARTITION = TopicPartition(TOPIC, 0)

private fun settings(): ReceiverSettings<String, String> =
  ReceiverSettings(
    bootstrapServers = "unused:9092",
    keyDeserializer = StringDeserializer(),
    valueDeserializer = StringDeserializer(),
    groupId = "commit-race-group",
    commitStrategy = CommitStrategy.BySize(1),
    commitRetryInterval = 50.milliseconds,
    closeTimeout = 5.seconds,
  )

/** Fails the first commit with [error], and lets every commit after it succeed. */
private fun failingTheFirstCommitWith(error: Exception): CommitFailingConsumer =
  CommitFailingConsumer(error).apply {
    updateBeginningOffsets(mapOf(PARTITION to 0L))
    schedulePollTask {
      rebalance(listOf(PARTITION))
      addRecord(ConsumerRecord(TOPIC, PARTITION.partition(), 0L, "key", "value"))
    }
  }

private class CommitFailingConsumer(private val error: Exception) :
  MockConsumer<String, String>(OffsetResetStrategy.EARLIEST) {

  private val attempts = AtomicInteger(0)
  val successfulCommits = CopyOnWriteArrayList<Map<TopicPartition, OffsetAndMetadata>>()

  override fun commitAsync(
    offsets: MutableMap<TopicPartition, OffsetAndMetadata>,
    callback: OffsetCommitCallback?,
  ) {
    requireNotNull(callback) { "the event loop always commits with a callback" }
    if (attempts.incrementAndGet() == 1) callback.onComplete(offsets, error)
    else {
      successfulCommits += offsets.toMap()
      callback.onComplete(offsets, null)
    }
  }
}

private class Collecting(
  private val job: Job,
  private val scope: CoroutineScope,
  private val dispatcher: ExecutorCoroutineDispatcher,
  val flowFailure: CompletableDeferred<Throwable>,
) {
  suspend fun close() {
    job.cancelAndJoin()
    scope.coroutineContext[Job]?.cancelAndJoin()
    dispatcher.close()
  }
}

private fun collect(consumer: MockConsumer<String, String>): Collecting {
  /* The event loop asserts that it runs on a thread named like the library's own dispatcher. */
  val dispatcher = Executors.newSingleThreadExecutor { runnable ->
    Thread(runnable, "kotlin-kafka-commit-race-group")
  }.asCoroutineDispatcher()
  val scope = CoroutineScope(Job() + dispatcher)
  val flowFailure = CompletableDeferred<Throwable>()

  val job = scope.launch {
    val loop = EventLoop(
      topicNames = setOf(TOPIC),
      settings = settings(),
      consumer = consumer,
      scope = scope,
      outerContext = currentCoroutineContext(),
    )
    loop.receive()
      .catch { e -> flowFailure.complete(e) }
      .collect { records ->
        records.forEach { record -> loop.offsetFromRecord(record).acknowledge() }
      }
  }

  return Collecting(job, scope, dispatcher, flowFailure)
}
