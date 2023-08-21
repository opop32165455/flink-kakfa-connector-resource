/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.kafka.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.kafka.source.enumerator.KafkaSourceEnumState;
import org.apache.flink.connector.kafka.source.enumerator.KafkaSourceEnumStateSerializer;
import org.apache.flink.connector.kafka.source.enumerator.KafkaSourceEnumerator;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.enumerator.subscriber.KafkaSubscriber;
import org.apache.flink.connector.kafka.source.metrics.KafkaSourceReaderMetrics;
import org.apache.flink.connector.kafka.source.reader.KafkaPartitionSplitReader;
import org.apache.flink.connector.kafka.source.reader.KafkaRecordEmitter;
import org.apache.flink.connector.kafka.source.reader.KafkaSourceReader;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.connector.kafka.source.reader.fetcher.KafkaSourceFetcherManager;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplitSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.UserCodeClassLoader;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collection;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * The Source implementation of Kafka. Please use a {@link KafkaSourceBuilder} to construct a {@link
 * KafkaSource}. The following example shows how to create a KafkaSource emitting records of <code>
 * String</code> type.
 *
 * <pre>{@code
 * KafkaSource<String> source = KafkaSource
 *     .<String>builder()
 *     .setBootstrapServers(KafkaSourceTestEnv.brokerConnectionStrings)
 *     .setGroupId("MyGroup")
 *     .setTopics(Arrays.asList(TOPIC1, TOPIC2))
 *     .setDeserializer(new TestingKafkaRecordDeserializationSchema())
 *     .setStartingOffsets(OffsetsInitializer.earliest())
 *     .build();
 * }</pre>
 *
 * <p>{@link org.apache.flink.connector.kafka.source.enumerator.KafkaSourceEnumerator} only supports
 * adding new splits and not removing splits in split discovery.
 *
 * <p>See {@link KafkaSourceBuilder} for more details on how to configure this source.
 *
 * @param <OUT> the output type of the source.
 */
@PublicEvolving
public class KafkaSource<OUT> implements Source<OUT, KafkaPartitionSplit, KafkaSourceEnumState>, ResultTypeQueryable<OUT> {
    private static final long serialVersionUID = -8755372893283732098L;
    // Users can choose only one of the following ways to specify the topics to consume from.
    private final KafkaSubscriber subscriber;
    // Users can specify the starting / stopping offset initializer.
    private final OffsetsInitializer startingOffsetsInitializer;
    private final OffsetsInitializer stoppingOffsetsInitializer;
    // 是否有界限 kafka一般无界 也可以指定offset起点终点来使用有界流
    private final Boundedness boundedness;
    private final KafkaRecordDeserializationSchema<OUT> deserializationSchema;
    // The configurations.
    private final Properties props;

    KafkaSource(KafkaSubscriber subscriber, OffsetsInitializer startingOffsetsInitializer, @Nullable OffsetsInitializer stoppingOffsetsInitializer, Boundedness boundedness, KafkaRecordDeserializationSchema<OUT> deserializationSchema, Properties props) {
        this.subscriber = subscriber;
        this.startingOffsetsInitializer = startingOffsetsInitializer;
        this.stoppingOffsetsInitializer = stoppingOffsetsInitializer;
        this.boundedness = boundedness;
        this.deserializationSchema = deserializationSchema;
        this.props = props;
    }

    /**
     * Get a kafkaSourceBuilder to build a {@link KafkaSource}.
     *
     * @return a Kafka source builder.
     */
    public static <OUT> KafkaSourceBuilder<OUT> builder() {
        return new KafkaSourceBuilder<>();
    }

    @Override
    public Boundedness getBoundedness() {
        return this.boundedness;
    }

    /**
     * todo 从数据源读取数据（读取核心方法）
     * 则负责具体partition数据的读取。读取或者拉取Enumerator分片后的数据
     * 1.运行在task manager上
     * 2.接收enumerator处理好的split数据
     * 3.主动拉取enumerator的split数据
     * 4.将split的数据，处理，然后从kafka读取
     */
    @Internal
    @Override
    public SourceReader<OUT, KafkaPartitionSplit> createReader(SourceReaderContext readerContext) throws Exception {
        return createReader(readerContext, (ignore) -> {
        });
    }

    @VisibleForTesting
    SourceReader<OUT, KafkaPartitionSplit> createReader(SourceReaderContext readerContext, Consumer<Collection<String>> splitFinishedHook) throws Exception {
        FutureCompletingBlockingQueue<RecordsWithSplitIds<ConsumerRecord<byte[], byte[]>>> elementsQueue = new FutureCompletingBlockingQueue<>();
        deserializationSchema.open(new DeserializationSchema.InitializationContext() {
            @Override
            public MetricGroup getMetricGroup() {
                return readerContext.metricGroup().addGroup("deserializer");
            }

            @Override
            public UserCodeClassLoader getUserCodeClassLoader() {
                return readerContext.getUserCodeClassLoader();
            }
        });
        final KafkaSourceReaderMetrics kafkaSourceReaderMetrics = new KafkaSourceReaderMetrics(readerContext.metricGroup());

        //从kafka 读取分区数据
        Supplier<KafkaPartitionSplitReader> splitReaderSupplier = () -> new KafkaPartitionSplitReader(props, readerContext, kafkaSourceReaderMetrics);
        // kafka record 处理器
        KafkaRecordEmitter<OUT> recordEmitter = new KafkaRecordEmitter<>(deserializationSchema);

        //用于提交kafka offset commit
        KafkaSourceFetcherManager fetcherManager =
                new KafkaSourceFetcherManager(elementsQueue,
                        splitReaderSupplier::get,
                        splitFinishedHook);

        return new KafkaSourceReader<>(elementsQueue,
                fetcherManager,
                recordEmitter,
                //核心配置
                toConfiguration(props),
                readerContext,
                kafkaSourceReaderMetrics);
    }

    /**
     * todo 分片枚举器 高并行 读取数据 （核心）
     * 1.负责发现需要读取的 kafka partition（负责发现需要读取的kafka partition）
     * 2.根据并行度切分任务，构造split，需要避开数据倾斜
     * 3.发送split到reader
     * 4.让reader能够主动拉取split
     */
    @Internal
    @Override
    public SplitEnumerator<KafkaPartitionSplit, KafkaSourceEnumState> createEnumerator(SplitEnumeratorContext<KafkaPartitionSplit> enumContext) {
        return new KafkaSourceEnumerator(subscriber, startingOffsetsInitializer, stoppingOffsetsInitializer, props, enumContext, boundedness);
    }
    //todo 恢复枚举器
    //能够从checkpoint恢复 继续执行分片
    @Internal
    @Override
    public SplitEnumerator<KafkaPartitionSplit, KafkaSourceEnumState> restoreEnumerator(SplitEnumeratorContext<KafkaPartitionSplit> enumContext, KafkaSourceEnumState checkpoint) throws IOException {
        return new KafkaSourceEnumerator(subscriber, startingOffsetsInitializer, stoppingOffsetsInitializer, props, enumContext, boundedness, checkpoint.assignedPartitions());
    }
    //序列化
    @Internal
    @Override
    public SimpleVersionedSerializer<KafkaPartitionSplit> getSplitSerializer() {
        return new KafkaPartitionSplitSerializer();
    }
    //反序列化
    @Internal
    @Override
    public SimpleVersionedSerializer<KafkaSourceEnumState> getEnumeratorCheckpointSerializer() {
        return new KafkaSourceEnumStateSerializer();
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return deserializationSchema.getProducedType();
    }

    // ----------- private helper methods ---------------

    private Configuration toConfiguration(Properties props) {
        Configuration config = new Configuration();
        props.stringPropertyNames().forEach(key -> config.setString(key, props.getProperty(key)));
        return config;
    }

    @VisibleForTesting
    Configuration getConfiguration() {
        return toConfiguration(props);
    }

    @VisibleForTesting
    KafkaSubscriber getKafkaSubscriber() {
        return subscriber;
    }

    @VisibleForTesting
    OffsetsInitializer getStoppingOffsetsInitializer() {
        return stoppingOffsetsInitializer;
    }
}
