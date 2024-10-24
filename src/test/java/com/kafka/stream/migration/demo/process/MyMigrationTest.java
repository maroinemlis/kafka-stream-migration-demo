package com.kafka.stream.migration.demo.process;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;

import java.time.Duration;
import java.util.Properties;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

@ActiveProfiles("test")
@SpringBootTest
class MyMigrationTest {

    @Autowired
    Topology topology;

    @Autowired
    Properties testProp;

    @Autowired
    KafkaProperties kafkaProperties;

    @Test
    void should_patch_state_store() {

        //Given
        testProp.putAll(kafkaProperties.getProperties());
        TopologyTestDriver topologyTestDriver = new TopologyTestDriver(topology, testProp);

        KeyValueStore<String, String> keyValueStore = topologyTestDriver.getKeyValueStore("my-state-store-name");
        var ids = Stream.iterate(0, i -> i + 1).limit(10).toList();
        ids.forEach(i -> keyValueStore.put("key" + i, "value" + i));

        //When stream advanced by one second
        topologyTestDriver.advanceWallClockTime(Duration.ofSeconds(1));

        //Then only key mod 2 = 0 is patched
        ids.stream().filter(i -> i % 2 == 0).forEach(i -> assertThat(keyValueStore.get("key" + i)).isEqualTo("value" + i + "patched"));
        ids.stream().filter(i -> i % 2 != 0).forEach(i -> assertThat(keyValueStore.get("key" + i)).isEqualTo("value" + i));

    }

}
