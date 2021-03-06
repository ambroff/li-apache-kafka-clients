/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.producer;

import com.linkedin.kafka.clients.common.ClusterDescriptor;
import com.linkedin.kafka.clients.common.ClusterGroupDescriptor;
import com.linkedin.kafka.clients.metadataservice.MetadataServiceClient;
import com.linkedin.kafka.clients.metadataservice.MetadataServiceClientException;

import com.linkedin.mario.common.websockets.MessageType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.assertFalse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The unit test for federated producer.
 */
public class LiKafkaFederatedProducerImplTest {
  private static final Logger LOG = LoggerFactory.getLogger(LiKafkaFederatedProducerImplTest.class);

  private static final String TOPIC1 = "topic1";
  private static final String TOPIC2 = "topic2";
  private static final String TOPIC3 = "topic3";
  private static final ClusterDescriptor CLUSTER1 = new ClusterDescriptor("cluster1", "url1", "zk1");
  private static final ClusterDescriptor CLUSTER2 = new ClusterDescriptor("cluster2", "url2", "zk2");
  private static final ClusterGroupDescriptor CLUSTER_GROUP = new ClusterGroupDescriptor("group", "env");

  private MetadataServiceClient _mdsClient;
  private LiKafkaFederatedProducerImpl<byte[], byte[]> _federatedProducer;

  private class MockProducerBuilder extends LiKafkaProducerBuilder<byte[], byte[]> {
    @Override
    public LiKafkaProducer<byte[], byte[]> build() {
      return new MockLiKafkaProducer();
    }
  }

  @BeforeMethod
  public void setup() {
    _mdsClient = Mockito.mock(MetadataServiceClient.class);

    Map<String, String> producerConfig = new HashMap<>();
    producerConfig.put(LiKafkaProducerConfig.CLUSTER_GROUP_CONFIG, CLUSTER_GROUP.getName());
    producerConfig.put(LiKafkaProducerConfig.CLUSTER_ENVIRONMENT_CONFIG, CLUSTER_GROUP.getEnvironment());
    producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
    producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");

    _federatedProducer = new LiKafkaFederatedProducerImpl<>(producerConfig, _mdsClient, new MockProducerBuilder());
  }

  @Test
  public void testBasicWorkflow() throws MetadataServiceClientException {
    // Set expectations so that topics 1 and 3 are hosted in cluster 1 and topic 2 in cluster 2.
    when(_mdsClient.getClusterForTopic(eq(TOPIC1), eq(CLUSTER_GROUP), anyInt())).thenReturn(CLUSTER1);
    when(_mdsClient.getClusterForTopic(eq(TOPIC2), eq(CLUSTER_GROUP), anyInt())).thenReturn(CLUSTER2);
    when(_mdsClient.getClusterForTopic(eq(TOPIC3), eq(CLUSTER_GROUP), anyInt())).thenReturn(CLUSTER1);

    // Make sure we start with a clean slate
    assertNull("Producer for cluster 1 should have not been created yet",
        _federatedProducer.getPerClusterProducer(CLUSTER1));
    assertNull("Producer for cluster 2 should have not been created yet",
        _federatedProducer.getPerClusterProducer(CLUSTER2));

    // Produce to all three topics
    ProducerRecord<byte[], byte[]> record1 = new ProducerRecord<>(TOPIC1, 0, 0L, "key1".getBytes(), "value1".getBytes());
    ProducerRecord<byte[], byte[]> record2 = new ProducerRecord<>(TOPIC2, 0, 0L, "key2".getBytes(), "value2".getBytes());
    ProducerRecord<byte[], byte[]> record3 = new ProducerRecord<>(TOPIC3, 0, 0L, "key3".getBytes(), "value3".getBytes());

    Future<RecordMetadata> metadata1 = _federatedProducer.send(record1);
    assertFalse("Send for topic 1 should not be done yet", metadata1.isDone());

    Future<RecordMetadata> metadata2 = _federatedProducer.send(record2);
    assertFalse("Send for topic 2 should not be done yet", metadata2.isDone());

    Future<RecordMetadata> metadata3 = _federatedProducer.send(record3);
    assertFalse("Send for topic 3 should not be done yet", metadata3.isDone());

    // Verify a correct producer is used for each send. Records 1 and 3 should be produced to cluster 1 and record 2 to
    // cluster 2.
    MockProducer producer1 = ((MockLiKafkaProducer) _federatedProducer.getPerClusterProducer(CLUSTER1)).getDelegate();
    MockProducer producer2 = ((MockLiKafkaProducer) _federatedProducer.getPerClusterProducer(CLUSTER2)).getDelegate();
    assertNotNull("Producer for cluster 1 should have been created", producer1);
    assertNotNull("Producer for cluster 2 should have been created", producer2);

    List<ProducerRecord> expectedHistory1 = new ArrayList<>(Arrays.asList(record1, record3));
    List<ProducerRecord> expectedHistory2 = new ArrayList<>(Arrays.asList(record2));
    assertEquals("Cluster1", expectedHistory1, producer1.history());
    assertEquals("Cluster2", expectedHistory2, producer2.history());

    // Verify per-cluster producers have not flushed yet.
    assertFalse("Producer for cluster 1 should have not been flushed", producer1.flushed());
    assertFalse("Producer for cluster 2 should have not been flushed", producer2.flushed());

    // Flush the federated producer and verify both producers flush.
    _federatedProducer.flush();
    assertTrue("Producer for cluster 1 should have been flushed", producer1.flushed());
    assertTrue("Producer for cluster 2 should have been flushed", producer2.flushed());

    // All sends should have completed without errors.
    assertTrue("Send for topic 1 should be immediately completed", metadata1.isDone());
    assertFalse("Send for topic 1 should be successful", isError(metadata1));

    assertTrue("Send for topic 2 should be immediately completed", metadata2.isDone());
    assertFalse("Send for topic 2 should be successful", isError(metadata2));

    assertTrue("Send for topic 3 should be immediately completed", metadata3.isDone());
    assertFalse("Send for topic 3 should be successful", isError(metadata3));

    // Verify per-cluster producers are not in closed state.
    assertFalse("Producer for cluster 1 should have not been closed", producer1.closed());
    assertFalse("Producer for cluster 2 should have not been closed", producer2.closed());

    // Close the federated producer and verify both producers are closed.
    _federatedProducer.close();
    assertTrue("Producer for cluster 1 should have been closed", producer1.closed());
    assertTrue("Producer for cluster 2 should have been closed", producer2.closed());
  }

  @Test
  public void testReloadConfigCommand() throws MetadataServiceClientException, InterruptedException {
    // Set expectations so that topics 1 and 3 are hosted in cluster 1 and topic 2 in cluster 2.
    when(_mdsClient.getClusterForTopic(eq(TOPIC1), eq(CLUSTER_GROUP), anyInt())).thenReturn(CLUSTER1);
    when(_mdsClient.getClusterForTopic(eq(TOPIC2), eq(CLUSTER_GROUP), anyInt())).thenReturn(CLUSTER2);
    when(_mdsClient.getClusterForTopic(eq(TOPIC3), eq(CLUSTER_GROUP), anyInt())).thenReturn(CLUSTER1);

    // Make sure we start with a clean slate
    assertNull("Producer for cluster 1 should have not been created yet",
        _federatedProducer.getPerClusterProducer(CLUSTER1));
    assertNull("Producer for cluster 2 should have not been created yet",
        _federatedProducer.getPerClusterProducer(CLUSTER2));

    // Produce to all three topics to instantiate per-cluster producers
    ProducerRecord<byte[], byte[]> record1 = new ProducerRecord<>(TOPIC1, 0, 0L, "key1".getBytes(), "value1".getBytes());
    ProducerRecord<byte[], byte[]> record2 = new ProducerRecord<>(TOPIC2, 0, 0L, "key2".getBytes(), "value2".getBytes());
    ProducerRecord<byte[], byte[]> record3 = new ProducerRecord<>(TOPIC3, 0, 0L, "key3".getBytes(), "value3".getBytes());

    // Simulate sending bootup configs from Conductor
    Map<String, String> bootupConfigs = new HashMap<>();
    bootupConfigs.put("K3", "V3");
    bootupConfigs.put("K4", "V4");

    _federatedProducer.applyBootupConfigFromConductor(bootupConfigs);

    _federatedProducer.send(record1);
    _federatedProducer.send(record2);
    _federatedProducer.send(record3);

    // Verify that after send, boot up configs have been successfully applied to the two producers
    Assert.assertEquals(_federatedProducer.getNumProducersWithBootupConfigs(), 2);

    _federatedProducer.flush();

    Map<String, String> newConfigs = new HashMap<>();
    newConfigs.put("K1", "V1");
    newConfigs.put("K2", "V2");
    UUID commandId = UUID.randomUUID();

    _federatedProducer.reloadConfig(newConfigs, commandId);

    // wait for reload config finish
    _federatedProducer.waitForReloadConfigFinish();

    // verify corresponding marioClient method is only called once
    verify(_mdsClient, times(1)).reportCommandExecutionComplete(eq(commandId), any(), eq(MessageType.RELOAD_CONFIG_RESPONSE), eq(true));
    verify(_mdsClient, times(1)).reRegisterFederatedClient(any());

    // verify per-cluster producers have been recreated after reloadConfig
    assertNotNull("Producer for cluster 1 should have been cleared",
        _federatedProducer.getPerClusterProducer(CLUSTER1));
    assertNotNull("Producer for cluster 2 should have been cleared",
        _federatedProducer.getPerClusterProducer(CLUSTER2));

    // verify after reload config, the common producer configs contains the new configs from config reload command
    assertTrue(_federatedProducer.getCommonProducerConfigs().originals().containsKey("K1"));
    assertTrue(_federatedProducer.getCommonProducerConfigs().originals().containsKey("K2"));

    // Simulate the boot up config response came after the producers have been created, this is essentially a config reload, so we expect the
    // config reload count to be 2
    _federatedProducer.applyBootupConfigFromConductor(bootupConfigs);
    Assert.assertEquals(_federatedProducer.getNumConfigReloads(), 2);

    // verify send is still successful after reload config
    Future<RecordMetadata> metadata1 = _federatedProducer.send(record1);
    Future<RecordMetadata> metadata2 = _federatedProducer.send(record2);
    Future<RecordMetadata> metadata3 = _federatedProducer.send(record3);

    _federatedProducer.flush();

    MockProducer producer1 = ((MockLiKafkaProducer) _federatedProducer.getPerClusterProducer(CLUSTER1)).getDelegate();
    MockProducer producer2 = ((MockLiKafkaProducer) _federatedProducer.getPerClusterProducer(CLUSTER2)).getDelegate();

    assertTrue("Producer for cluster 1 should have been flushed", producer1.flushed());
    assertTrue("Producer for cluster 2 should have been flushed", producer2.flushed());

    assertFalse("Producer for cluster 1 should have not been closed", producer1.closed());
    assertFalse("Producer for cluster 2 should have not been closed", producer2.closed());

    // All sends should have completed without errors.
    assertTrue("Send for topic 1 should be immediately completed", metadata1.isDone());
    assertFalse("Send for topic 1 should be successful", isError(metadata1));

    assertTrue("Send for topic 2 should be immediately completed", metadata2.isDone());
    assertFalse("Send for topic 2 should be successful", isError(metadata2));

    assertTrue("Send for topic 3 should be immediately completed", metadata3.isDone());
    assertFalse("Send for topic 3 should be successful", isError(metadata3));
  }

  private boolean isError(Future<?> future) {
    try {
      future.get();
      return false;
    } catch (Exception e) {
      return true;
    }
  }
}
