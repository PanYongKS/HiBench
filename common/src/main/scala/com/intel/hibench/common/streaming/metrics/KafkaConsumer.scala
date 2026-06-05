/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.intel.hibench.common.streaming.metrics

import java.util.{Properties, Collections}
import java.time.Duration
import org.apache.kafka.clients.consumer.{KafkaConsumer => NewConsumer, ConsumerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import scala.collection.JavaConverters._

class KafkaConsumer(zookeeperConnect: String, topic: String, partition: Int) {

  private val CLIENT_ID = "metrics_reader"

  private val bootstrapServers = MetricsUtil.getBootstrapServers(zookeeperConnect)

  private val props = new Properties()
  props.put("bootstrap.servers", bootstrapServers)
  props.put("group.id", CLIENT_ID)
  props.put("key.deserializer", classOf[ByteArrayDeserializer].getName)
  props.put("value.deserializer", classOf[ByteArrayDeserializer].getName)
  props.put("enable.auto.commit", "false")
  props.put("auto.offset.reset", "earliest")
  props.put("max.poll.records", "500")

  private val consumer = new NewConsumer[Array[Byte], Array[Byte]](props)
  private val topicPartition = new TopicPartition(topic, partition)

  consumer.assign(Collections.singletonList(topicPartition))

  private val earliestOffset: Long = {
    val offsets = consumer.beginningOffsets(Collections.singletonList(topicPartition))
    offsets.get(topicPartition)
  }
  private var nextOffset: Long = earliestOffset
  consumer.seek(topicPartition, nextOffset)

  private var iterator: Iterator[ConsumerRecord[Array[Byte], Array[Byte]]] = getIterator()

  def next(): Array[Byte] = {
    val record = iterator.next()
    nextOffset = record.offset() + 1
    record.value()
  }

  def hasNext: Boolean = {
    @annotation.tailrec
    def hasNextHelper(iter: Iterator[ConsumerRecord[Array[Byte], Array[Byte]]], newIterator: Boolean): Boolean = {
      if (iter.hasNext) true
      else if (newIterator) false
      else {
        iterator = getIterator()
        hasNextHelper(iterator, newIterator = true)
      }
    }
    hasNextHelper(iterator, newIterator = false)
  }

  def close(): Unit = {
    consumer.close()
  }

  private def getIterator(): Iterator[ConsumerRecord[Array[Byte], Array[Byte]]] = {
    consumer.seek(topicPartition, nextOffset)
    val records = consumer.poll(Duration.ofMillis(1000))
    records.records(topicPartition).iterator().asScala
  }
}
