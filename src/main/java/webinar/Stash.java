package webinar;

import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.datamodel.TimestampedEntry;
import com.hazelcast.jet.datamodel.TimestampedItem;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.StreamStage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map.Entry;

import static com.hazelcast.jet.Traversers.traverseArray;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.Util.mapEventNewValue;
import static com.hazelcast.jet.Util.mapPutEvents;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.function.DistributedComparator.comparing;
import static com.hazelcast.jet.function.DistributedFunctions.wholeItem;
import static com.hazelcast.jet.pipeline.ContextFactories.replicatedMapContext;
import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_OLDEST;
import static com.hazelcast.jet.pipeline.Sinks.map;
import static com.hazelcast.jet.pipeline.Sources.mapJournal;
import static com.hazelcast.jet.pipeline.WindowDefinition.sliding;
import static com.hazelcast.jet.pipeline.WindowDefinition.tumbling;
import static java.util.stream.Collectors.toList;

public class Stash {
    static final int PUBLISH_KEY = 42;
    static final String TWEETS = "tweets";
    static final String TOP_LIST = "top-list";
    static final String STOPWORDS = "stopwords";
    static final String SAMPLES_HOME =
            "/Users/mtopol/dev/java/hazelcast-jet-code-samples/sample-data/src/main/resources";

    private static Pipeline buildPipeline() {
        Pipeline p = Pipeline.create();

        StreamStage<String> tweets = p.<Tweet>drawFrom(
                mapJournal(TWEETS, mapPutEvents(), mapEventNewValue(), START_FROM_OLDEST))
                .addTimestamps(Tweet::timestamp, 10)
                .map(Tweet::text);

        StreamStage<String> words = tweets
                .flatMap(line -> traverseArray(line.toLowerCase().split("\\W+")))
                .filterUsingContext(replicatedMapContext(STOPWORDS),
                        (stopwords, word) -> !word.isEmpty() && !stopwords.containsKey(word));

        StreamStage<TimestampedEntry<String, Long>> wordFrequencies = words
                .window(sliding(10_000, 100))
                .groupingKey(wholeItem())
                .aggregate(counting());

        StreamStage<TimestampedItem<List<String>>> topLists = wordFrequencies
                .window(tumbling(100))
                .aggregate(TweetPublisher.topN(10, comparing(Entry::getValue)),
                        (winStart, winEnd, topList) -> new TimestampedItem<>(winEnd,
                                topList.stream().map(Entry::getKey).collect(toList())));

        topLists.map(timestampedTopList -> entry(PUBLISH_KEY, timestampedTopList))
                .drainTo(map(TOP_LIST));
        return p;
    }

    static void loadStopwordsIntoReplicatedMap(JetInstance jet) throws IOException {
        ReplicatedMap<String, Integer> swMap = jet.getHazelcastInstance().getReplicatedMap(STOPWORDS);
        Files.lines(Paths.get(SAMPLES_HOME + "/stopwords.txt")).forEach(sw -> swMap.put(sw, 0));
    }
}

