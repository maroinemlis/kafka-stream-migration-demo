package com.kafka.stream.migration.demo.process.process;

import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;

public class MyMigrationProcess extends ContextualProcessor<Object, Object, String, String> {

    private final String storeName;
    private final Duration punctuateIntervalDuration;
    private boolean isPunctuatedOnce;

    private KeyValueStore<String, String> store;

    public MyMigrationProcess(String storeName, Duration punctuateIntervalDuration) {
        this.storeName = storeName;
        this.punctuateIntervalDuration = punctuateIntervalDuration;
        this.isPunctuatedOnce = false;
    }

    @Override
    public void init(ProcessorContext<String, String> processorContext) {
        super.init(processorContext);
        store = processorContext.getStateStore(storeName);
        processorContext.schedule(punctuateIntervalDuration, PunctuationType.WALL_CLOCK_TIME, this::punctuate);
    }

    private void punctuate(long clockTime) {
        if (isPunctuatedOnce) {
            return;
        }
        this.isPunctuatedOnce = true;
        var iterator = store.all();
        while (iterator.hasNext()) {
            var next = iterator.next();
            if (Integer.parseInt(next.key.replace("key", "")) % 2 == 0) {
                String patchValue = patchValue(next.value);
                store.put(next.key, patchValue);
            }

        }
    }

    private String patchValue(String value) {
        return value + "patched";
    }

    @Override
    public void process(Record<Object, Object> emptyRecord) {
        //Do nothing
    }

}
