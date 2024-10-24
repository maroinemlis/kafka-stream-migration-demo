package com.kafka.stream.migration.demo.process.config;

import com.kafka.stream.migration.demo.process.process.MyMigrationProcess;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.util.regex.Pattern;


@Configuration
public class TopologyConfig {

    private static final String STORE_NAME = "my-state-store-name";

    private final StreamsBuilder streamsBuilder;

    public TopologyConfig(StreamsBuilder streamsBuilder) {
        this.streamsBuilder = streamsBuilder;
    }

    @Bean
    Topology topology() {
        addStateStore();
        migrateProcess();
        return streamsBuilder.build();
    }

    void addStateStore() {
        StoreBuilder<KeyValueStore<String, String>> store = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(STORE_NAME),
                Serdes.String(),
                Serdes.String()
        );
        streamsBuilder.addStateStore(store);
    }

    void migrateProcess() {
        streamsBuilder.stream(Pattern.compile("dummy-topic-juste-to-generate-stream"))
                .process(() -> new MyMigrationProcess(STORE_NAME, Duration.ofSeconds(1)), STORE_NAME);
    }

}