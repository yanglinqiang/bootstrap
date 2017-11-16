package com.ylq.framework.kafka;

import com.datastax.driver.core.ConsistencyLevel;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ylq.framework.ILoader;
import com.ylq.framework.scylladb.Write;
import com.ylq.framework.support.ConfigUtil;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


/**
 * Created by ylq on 2017/11/16.
 */
public class ReaderTest implements ILoader {
    private static final Logger logger = LogManager.getLogger(ReaderTest.class);

    private List<KafkaStream<byte[], byte[]>> streams;

    public ConsumerConfig getConsumerConfig() {
        Properties props = new Properties();
        props.put("zookeeper.connect", ConfigUtil.getString("kafka.zookeeper.connect"));
        props.put("group.id", ConfigUtil.getString("kafka.group.id"));
        props.put("zookeeper.session.timeout.ms", ConfigUtil.getString("kafka.zookeeper.session.timeout.ms"));
        props.put("zookeeper.sync.time.ms", ConfigUtil.getString("kafka.zookeeper.sync.time.ms"));
        props.put("auto.commit.interval.ms", ConfigUtil.getString("kafka.auto.commit.interval.ms"));
        return new ConsumerConfig(props);
    }

    @Override
    public void init() {
        ConsumerConnector consumerConnector = kafka.consumer.Consumer
                .createJavaConsumerConnector(getConsumerConfig());
        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put("game_html5_tdcv3", 9);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);
        streams = consumerMap.get("game_html5_tdcv3");
    }

    @Override
    public void start() {
        ExecutorService executorService = Executors.newFixedThreadPool(9);
        for (KafkaStream<byte[], byte[]> stream : streams) {
            executorService.execute(createThread(stream));
        }
    }

    private Thread createThread(KafkaStream<byte[], byte[]> stream) {
        return new Thread() {
            ObjectMapper objectMapper = new ObjectMapper();

            @Override
            public void run() {
                ConsumerIterator<byte[], byte[]> it = stream.iterator();
                while (it.hasNext()) {
                    String str = new String(it.next().message());
                    Integer splitIndex = str.indexOf("\0");
                    if (splitIndex <= 0) {
                        continue;
                    }
                    try {
                        Map<String, Object> header = objectMapper.readValue(str.substring(0, splitIndex), Map.class);
                        long ts = Long.parseLong(header.get("ts").toString());
                        if (ts % 100000 == 0) {
                            logger.info(str);
                        }
                    } catch (IOException e) {
                        logger.error(e.getMessage(), e);
                    }
                }
            }
        };
    }
}
