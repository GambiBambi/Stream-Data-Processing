package com.example.bigdata;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.example.bigdata.CsvReader.readCsv;

public class ApacheLogToAlertRequests {

    public static String cleanString(String str) {
        return str.strip().replaceAll("[^a-zA-Z0-9 ]",  "");
    }

    public static List<List<String>> csv_data;

    public static void main(String[] args) throws Exception {
        csv_data = readCsv();

        Properties config = new Properties();
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, args[0]);
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "chicago-crimes-application");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        config.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MyEventTimeExtractor.class);

        Pattern pattern = Pattern.compile("Category: (.+), District: (\\d+\\.\\d+), Date: (.+), All: (\\d+), Arrest: (\\d+), Domestic: (\\d+), FBI: (\\d+)");
        Pattern pattern2 = Pattern.compile("IUCR: (.+), Category: (.+), District: (\\d+\\.\\d+), Date: (.+), Arrest: (true|false), Domestic: (true|false), IndexCode: (\\S)");

        Duration gracePeriod = Duration.ofDays(1);

        final Serde<String> stringSerde = Serdes.String();
        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> textLines = builder
                .stream("chicago-data", Consumed.with(stringSerde, stringSerde));

        KStream<String, AccessLogRecord> crimeStream = textLines
                .filter((key, value) -> AccessLogRecord.lineIsCorrect(value))
                .mapValues(value -> {
                            AccessLogRecord log = AccessLogRecord.parseFromLogLine(value);
                            return log;
                        });

        KTable<Windowed<String>, String> chicagoAggregate = crimeStream
                .map((key,value) -> {
                    String newValue = String.format("IUCR: %s, Category: %s, District: %f, Date: %s, Arrest: %b, Domestic: %b, IndexCode: %s", value.getIUCR(), value.getCategory(), value.getDistrict(), value.getMonthYear(), value.getArrest(), value.getDomestic(), value.getIndexCode());
                    return KeyValue.pair(value.getCategory() + "-" + value.getDistrict(), newValue);
                })
                .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofDays(30)).grace(gracePeriod))
                .aggregate(
                        () -> {
                            String iucr = "";
                            String category = "0000-00";
                            Float district = 0f;
                            String date = "";
                            Long all = 0L;
                            Long arrest = 0L;
                            Long domestic = 0L;
                            Long fbi = 0L;
                            String aggregate = String.format("IUCR: %s, Category: %s, District: %f, Date: %s, All: %d, Arrest: %d, Domestic: %d, FBI: %d", iucr, category, district, date, all, arrest, domestic, fbi);
                            return aggregate;
                            },
                        (aggKey, newValue, aggregate) -> {
                            String iucr = "";
                            String category = "";
                            Float district = 0f;
                            String date = "0000-00";
                            Boolean arrestValue = false;
                            Boolean domesticValue = false;
                            String indexCode = "";
                            Long all = 0L;
                            Long arrest = 0L;
                            Long domestic = 0L;
                            Long fbi = 0L;

                            Matcher matcher = pattern.matcher(aggregate);
                            if (matcher.find()) {
                                all = Long.parseLong(matcher.group(4));
                                arrest = Long.parseLong(matcher.group(5));
                                domestic = Long.parseLong(matcher.group(6));
                                fbi = Long.parseLong(matcher.group(7));
                            }

                            Matcher matcher2 = pattern2.matcher(newValue);
                            if (matcher2.find()) {
                                iucr = matcher2.group(1);
                                category = matcher2.group(2);
                                district = Float.parseFloat(matcher2.group(3));
                                date = matcher2.group(4);
                                arrestValue = Boolean.parseBoolean(matcher2.group(5));
                                domesticValue = Boolean.parseBoolean(matcher2.group(6));
                                indexCode = matcher2.group(7);
                            }

                            if (category.equals("No matching category")) {
                                System.out.println("----------------------------------Error------------------------------------------------------");
                                System.out.println("IUCR:" + iucr);
                                System.out.println("Category: " + category);
                                System.out.println("District: " + district);
                                System.out.println("Aggregate: " + aggregate);
                                System.out.println("newValue: " + newValue);
                            }

                            all=all+1;

                            if(arrestValue == Boolean.TRUE) {
                                arrest=arrest+1;
                            }
                            if(domesticValue == Boolean.TRUE) {
                                domestic=domestic+1;
                            }
                            if(indexCode.equals("I")) {
                                fbi=fbi+1;
                            }

                            aggregate = String.format("Category: %s, District: %f, Date: %s, All: %d, Arrest: %d, Domestic: %d, FBI: %d", category, district, date, all, arrest, domestic, fbi);
                            return aggregate;
                        },
                        Materialized.with(stringSerde, stringSerde)
                )//;
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded())); //ten fragment dotyczy tylko trybu C

        KStream<String, String> result = chicagoAggregate.toStream()
                .map((k, v) -> KeyValue.pair(k.key(), v));
        result.to("count", Produced.with(stringSerde, stringSerde));

        KStream<String, String> json = chicagoAggregate.toStream()
                .map((k, v) -> {
                    String category = "";
                    Float district = 0f;
                    String date = "0000-00";
                    Long all = 0L;
                    Long arrest = 0L;
                    Long domestic = 0L;
                    Long fbi = 0L;
                    Matcher matcher = pattern.matcher(v);
                    if (matcher.find()) {
                        category = matcher.group(1);
                        district = Float.parseFloat(matcher.group(2));
                        date = matcher.group(3);
                        all = Long.parseLong(matcher.group(4));
                        arrest = Long.parseLong(matcher.group(5));
                        domestic = Long.parseLong(matcher.group(6));
                        fbi = Long.parseLong(matcher.group(7));
                    }
                    return KeyValue.pair(k.key(),
                            String.format(
                                    "{ \"schema\": { " +
                                            "\"type\": \"struct\", " +
                                            "\"optional\": false, " +
                                            "\"version\": 1, " +
                                            "\"fields\": [ " +
                                                "{ \"field\": \"category\", \"type\": \"string\", \"optional\": true }, " +
                                                "{ \"field\": \"district\", \"type\": \"float\", \"optional\": true }, " +
                                                "{ \"field\": \"date\", \"type\": \"string\", \"optional\": true }, " +
                                                "{ \"field\": \"all_count\", \"type\": \"int32\", \"optional\": true }, " +
                                                "{ \"field\": \"arrest\", \"type\": \"int32\", \"optional\": true }, " +
                                                "{ \"field\": \"domestic\", \"type\": \"int32\", \"optional\": true }, " +
                                                "{ \"field\": \"fbi\", \"type\": \"int32\", \"optional\": true } " +
                                                "] " +
                                            "}, " +
                                            "\"payload\": { " +
                                                "\"category\":\"%s\", " +
                                                "\"district\": %f, " +
                                                "\"date\": \"%s\", " +
                                                "\"all_count\": %d, " +
                                                "\"arrest\": %d, " +
                                                "\"domestic\": %d, " +
                                                "\"fbi\": %d " +
                                            "} " +
                                    "}",
                            category, district, date, all, arrest, domestic, fbi));
                });
        json.to("json", Produced.with(stringSerde, stringSerde));



        final Topology topology = builder.build();
        System.out.println(topology.describe());

        KafkaStreams streams = new KafkaStreams(topology, config);
        final CountDownLatch latch = new CountDownLatch(1);

        streams.setUncaughtExceptionHandler((thread, exception) -> {
            System.out.println(exception.getMessage());
        });

        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                cleanupInternalTopics();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
    private static void cleanupInternalTopics() {
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            // List all topics
            ListTopicsOptions options = new ListTopicsOptions();
            options.listInternal(true); // Include internal topics
            KafkaFuture<Set<String>> topicsFuture = adminClient.listTopics(options).names();
            Set<String> topics = topicsFuture.get();

            // Filter internal topics related to your application
            topics.removeIf(topic -> !topic.startsWith("chicago-crimes"));

            // Delete internal topics
            if (!topics.isEmpty()) {
                DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(topics);
                deleteTopicsResult.all().get(); // Wait for deletion to complete
                System.out.println("Deleted topics: " + topics);
            } else {
                System.out.println("No internal topics to delete.");
            }
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }
}

