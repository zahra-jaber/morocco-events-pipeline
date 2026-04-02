package com.morocco.events;

import com.morocco.events.deserializer.EventDeserializer;
import com.morocco.events.model.CityStatistics;
import com.morocco.events.model.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FlinkEventsProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkEventsProcessor.class);
    private static final String KAFKA_SERVERS = "kafka:29092";
    private static final String KAFKA_TOPIC = "raw_events";
    private static final String KAFKA_GROUP = "flink-processor";

    public static void main(String[] args) throws Exception {
        // 1. Créer l'environnement d'exécution
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        // 2. Configurer la source Kafka
        KafkaSource<Event> kafkaSource = KafkaSource.<Event>builder()
                .setBootstrapServers(KAFKA_SERVERS)
                .setTopics(KAFKA_TOPIC)
                .setGroupId(KAFKA_GROUP)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new EventDeserializer())
                .build();

        // 3. Créer le stream depuis Kafka (Flink = Consumer)
        DataStream<Event> eventStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.<Event>forMonotonousTimestamps()
                    .withTimestampAssigner((event, recordTimestamp) -> 
                        event.getTimestamp() > 0 ? event.getTimestamp() * 1000 : System.currentTimeMillis())
                    .withIdleness(Duration.ofSeconds(1)),  // Réduit de 5 à 1 seconde
                "Kafka Source"
        );

        LOG.info("✓ Flink Consumer connecté à Kafka: {} (topic: {})", KAFKA_SERVERS, KAFKA_TOPIC);

        // 4. Pipeline de transformation
        SingleOutputStreamOperator<CityStatistics> aggregatedStream = eventStream
                .keyBy(Event::getCity)
                .window(TumblingEventTimeWindows.of(Time.seconds(2)))  // Fenêtres de 2 sec au lieu de 10
                .aggregate(
                    new EventAggregateFunction(),
                    new EventWindowFunction()
                );

        // 5. Sink: Imprimer les résultats
        aggregatedStream.print()
                .setParallelism(1);

        // 6. Exécuter le job
        env.execute("Flink Morocco Events Pipeline");
    }

    /**
     * Fonction d'agrégation
     */
    public static class EventAggregateFunction 
            implements AggregateFunction<Event, EventAccumulator, EventAccumulator> {

        @Override
        public EventAccumulator createAccumulator() {
            return new EventAccumulator();
        }

        @Override
        public EventAccumulator add(Event event, EventAccumulator accumulator) {
            accumulator.addEvent(event);
            return accumulator;
        }

        @Override
        public EventAccumulator getResult(EventAccumulator accumulator) {
            return accumulator;
        }

        @Override
        public EventAccumulator merge(EventAccumulator acc1, EventAccumulator acc2) {
            acc1.merge(acc2);
            return acc1;
        }
    }

    /**
     * Classe accumulateur
     */
    public static class EventAccumulator {
        public List<Event> events = new ArrayList<>();
        public double totalPrice = 0.0;
        public double maxPrice = Double.MIN_VALUE;
        public double minPrice = Double.MAX_VALUE;
        public Map<String, Integer> categoryCounts = new HashMap<>();

        public void addEvent(Event event) {
            events.add(event);
            totalPrice += event.getPrice();
            maxPrice = Math.max(maxPrice, event.getPrice());
            minPrice = Math.min(minPrice, event.getPrice());
            
            String category = event.getCategory();
            categoryCounts.put(
                category,
                categoryCounts.getOrDefault(category, 0) + 1
            );
        }

        public void merge(EventAccumulator other) {
            this.events.addAll(other.events);
            this.totalPrice += other.totalPrice;
            this.maxPrice = Math.max(this.maxPrice, other.maxPrice);
            this.minPrice = Math.min(this.minPrice, other.minPrice);
            
            other.categoryCounts.forEach((k, v) ->
                this.categoryCounts.put(k, 
                    this.categoryCounts.getOrDefault(k, 0) + v)
            );
        }
    }

    /**
     * Fonction de fenêtre pour formater la sortie
     */
    public static class EventWindowFunction 
            implements WindowFunction<EventAccumulator, CityStatistics, String, TimeWindow> {

        @Override
        public void apply(String city, TimeWindow window, 
                         Iterable<EventAccumulator> input, 
                         Collector<CityStatistics> out) {
            
            EventAccumulator accumulator = input.iterator().next();
            
            CityStatistics stats = new CityStatistics(city);
            stats.setTotalEvents(accumulator.events.size());
            stats.setMaxPrice(accumulator.maxPrice == Double.MIN_VALUE ? 0 : accumulator.maxPrice);
            stats.setMinPrice(accumulator.minPrice == Double.MAX_VALUE ? 0 : accumulator.minPrice);
            
            if (accumulator.events.size() > 0) {
                double avgPrice = accumulator.totalPrice / accumulator.events.size();
                stats.setAvgPrice(avgPrice);
            }
            
            Map<String, Long> categories = new HashMap<>();
            accumulator.categoryCounts.forEach((k, v) -> categories.put(k, (long) v));
            stats.setCategoryCount(categories);
            
            out.collect(stats);
        }
    }
}