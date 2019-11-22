package com.linkedin.kafka.clients.consumer;

import com.linkedin.kafka.clients.auditing.NoOpAuditor;
import com.linkedin.kafka.clients.consumer.LiKafkaConsumerImpl;
import com.linkedin.kafka.clients.largemessage.DefaultSegmentDeserializer;
import com.linkedin.kafka.clients.largemessage.DefaultSegmentSerializer;
import com.linkedin.kafka.clients.largemessage.LargeMessageSegment;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static com.linkedin.kafka.clients.producer.LiKafkaProducerConfig.LARGE_MESSAGE_ENABLED_CONFIG;
import static com.linkedin.kafka.clients.producer.LiKafkaProducerConfig.LARGE_MESSAGE_SEGMENT_WRAPPING_REQUIRED_CONFIG;

public class LargeMessageEnvelopeConsumerBenchmark {
  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public void processNormalMessage(NormalMessageState state, Blackhole blackhole) {
    consumeLotsOfMessages(state.consumer, blackhole);
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public void processSingleSegmentLargeMessages(LargeMessageState state, Blackhole blackhole) {
    consumeLotsOfMessages(state.consumer, blackhole);
  }

  private static void consumeLotsOfMessages(Consumer<byte[], byte[]> consumer, Blackhole blackhole) {
    for (int i = 0; i < 100; i++) {
      ConsumerRecords<byte[], byte[]> results = consumer.poll(Duration.ofMillis(0));
      blackhole.consume(results);
    }
  }

  private static Consumer<byte[], byte[]> constructConsumer(TopicPartition topicPartition, List<ConsumerRecord<byte[], byte[]>> records) {
    Properties consumerProperties = new Properties();
    consumerProperties.setProperty(LARGE_MESSAGE_ENABLED_CONFIG, "true");
    consumerProperties.setProperty(LARGE_MESSAGE_SEGMENT_WRAPPING_REQUIRED_CONFIG, "true");
    return new LiKafkaConsumerImpl<>(
      new LiKafkaConsumerConfig(consumerProperties),
      new ByteArrayDeserializer(),
      new ByteArrayDeserializer(),
      new DefaultSegmentDeserializer(),
      new NoOpAuditor<>(),
      new FakeConsumer(topicPartition, records));
  }

  /**
   * Populate a consumer with ConsumerRecord objects that will be returned every time poll() is called.
   */
  @State(Scope.Benchmark)
  public static class NormalMessageState {
    Consumer<byte[], byte[]> consumer = null;

    @Setup
    public void setUp() {
      List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
      final String topicName = "topic1";

      for (int i = 0; i < 1000; i++) {
        byte[] message = ("hello " + i).getBytes();
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(topicName, 0, i, null, message);
        records.add(record);
      }

      consumer = constructConsumer(new TopicPartition(topicName, 0), records);
    }
  }

  /**
   * Same as NormalMessageBenchmark, except each message is wrapped in the large message segment envelope
   */
  @State(Scope.Benchmark)
  public static class LargeMessageState {
    Consumer<byte[], byte[]> consumer = null;

    @Setup
    public void setUp() {
      List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();

      final String topicName = "topic1";
      DefaultSegmentSerializer serializer = new DefaultSegmentSerializer();

      for (int i = 0; i < 1000; i++) {
        byte[] message = ("hello " + i).getBytes();
        LargeMessageSegment segment = new LargeMessageSegment(UUID.randomUUID(), 0, 1, message.length, ByteBuffer.wrap(message));
        byte[] wrappedMessage = serializer.serialize(topicName, segment);

        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(topicName, 0, i, null, wrappedMessage);
        records.add(record);
      }

      consumer = constructConsumer(new TopicPartition(topicName, 0), records);
    }
  }

  private static class FakeConsumer implements Consumer<byte[], byte[]> {
    private final TopicPartition _topicPartition;
    private final List<ConsumerRecord<byte[], byte[]>> _records;

    FakeConsumer(TopicPartition topicPartition, List<ConsumerRecord<byte[], byte[]>> records) {
      _topicPartition = topicPartition;
      _records = records;
    }

    @Override
    public Set<TopicPartition> assignment() {
      return Collections.singleton(_topicPartition);
    }

    @Override
    public Set<String> subscription() {
      return null;
    }

    @Override
    public void subscribe(Collection<String> topics) {
    }

    @Override
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener callback) {
    }

    @Override
    public void assign(Collection<TopicPartition> partitions) {
    }

    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener callback) {
    }

    @Override
    public void subscribe(Pattern pattern) {
    }

    @Override
    public void unsubscribe() {
    }

    @Override
    public ConsumerRecords<byte[], byte[]> poll(long timeout) {
      return new ConsumerRecords<>(Collections.singletonMap(_topicPartition, _records));
    }

    @Override
    public ConsumerRecords<byte[], byte[]> poll(Duration timeout) {
      return poll(timeout.toMillis());
    }

    @Override
    public void commitSync() {
    }

    @Override
    public void commitSync(Duration timeout) {
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {
    }

    @Override
    public void commitAsync() {
    }

    @Override
    public void commitAsync(OffsetCommitCallback callback) {
    }

    @Override
    public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
    }

    @Override
    public void seek(TopicPartition partition, long offset) {
    }

    @Override
    public void seekToBeginning(Collection<TopicPartition> partitions) {
    }

    @Override
    public void seekToEnd(Collection<TopicPartition> partitions) {
    }

    @Override
    public long position(TopicPartition partition) {
      return 0;
    }

    @Override
    public long position(TopicPartition partition, Duration timeout) {
      return 0;
    }

    @Override
    public OffsetAndMetadata committed(TopicPartition partition) {
      return null;
    }

    @Override
    public OffsetAndMetadata committed(TopicPartition partition, Duration timeout) {
      return null;
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
      return Collections.emptyMap();
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
      return null;
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic, Duration timeout) {
      return null;
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics() {
      return null;
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics(Duration timeout) {
      return null;
    }

    @Override
    public Set<TopicPartition> paused() {
      return Collections.emptySet();
    }

    @Override
    public void pause(Collection<TopicPartition> partitions) {
    }

    @Override
    public void resume(Collection<TopicPartition> partitions) {
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
      return null;
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch, Duration timeout) {
      return null;
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
      return null;
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Duration timeout) {
      return null;
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
      return null;
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Duration timeout) {
      return null;
    }

    @Override
    public void close() {
    }

    @Override
    public void close(long timeout, TimeUnit unit) {
    }

    @Override
    public void close(Duration timeout) {
    }

    @Override
    public void wakeup() {
    }
  }

  public static void main(String[] args) throws Exception {
    org.openjdk.jmh.Main.main(args);
  }
}
