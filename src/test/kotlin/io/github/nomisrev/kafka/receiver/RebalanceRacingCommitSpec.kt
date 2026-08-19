package io.github.nomisrev.kafka.receiver

import io.github.nomisRev.kafka.receiver.CommitStrategy
import io.github.nomisRev.kafka.receiver.ReceiverSettings
import io.github.nomisRev.kafka.receiver.internals.EventLoop
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.coroutineContext
import kotlin.test.assertEquals
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.MockConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.consumer.OffsetCommitCallback
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.RebalanceInProgressException
import org.apache.kafka.common.serialization.StringDeserializer
import org.junit.jupiter.api.Test

class RebalanceRacingCommitSpec {

  @Test
  fun `a commit racing a rebalance is retried instead of failing the receive flow`() = runBlocking {
    val topic = "commit-race-topic"
    val partition = TopicPartition(topic, 0)
    val commitAttempts = AtomicInteger(0)
    val successfulCommits = CopyOnWriteArrayList<Map<TopicPartition, OffsetAndMetadata>>()

    // the first commit races a rebalance, every following commit succeeds
    val consumer = object : MockConsumer<String, String>(OffsetResetStrategy.EARLIEST) {
      override fun commitAsync(
        offsets: MutableMap<TopicPartition, OffsetAndMetadata>,
        callback: OffsetCommitCallback?,
      ) {
        if (commitAttempts.incrementAndGet() == 1) {
          callback?.onComplete(offsets, RebalanceInProgressException("commit raced the rebalance"))
        } else {
          successfulCommits.add(offsets.toMap())
          callback?.onComplete(offsets, null)
        }
      }
    }
    consumer.updateBeginningOffsets(mapOf(partition to 0L))
    consumer.schedulePollTask {
      consumer.rebalance(listOf(partition))
      consumer.addRecord(ConsumerRecord(topic, 0, 0L, "key", "value"))
    }

    val settings = ReceiverSettings(
      bootstrapServers = "unused:9092",
      keyDeserializer = StringDeserializer(),
      valueDeserializer = StringDeserializer(),
      groupId = "commit-race-group",
      commitStrategy = CommitStrategy.BySize(1),
    )

    // the event loop asserts that it runs on a thread named like the library's own dispatcher
    val dispatcher = Executors.newSingleThreadExecutor { r ->
      Thread(r, "kotlin-kafka-commit-race-group")
    }.asCoroutineDispatcher()
    val job = Job()
    val scope = CoroutineScope(job + dispatcher)
    try {
      val loop = EventLoop(setOf(topic), settings, consumer, scope, coroutineContext)
      val firstRecord = CompletableDeferred<ConsumerRecord<String, String>>()
      val collector = launch {
        loop.receive().collect { records ->
          records.forEach { record -> firstRecord.complete(record) }
        }
      }

      withTimeout(30.seconds) {
        loop.offsetFromRecord(firstRecord.await()).acknowledge()
        while (successfulCommits.isEmpty()) delay(50)
      }
      collector.cancelAndJoin()

      // without retrying the racing commit, the collector would have failed with
      // RebalanceInProgressException before any successful commit could happen
      assertEquals(1L, successfulCommits.first()[partition]?.offset())
    } finally {
      job.cancelAndJoin()
      dispatcher.close()
    }
  }
}
