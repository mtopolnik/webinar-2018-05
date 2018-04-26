package webinar;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.StreamStage;

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
import static webinar.Stash.*;

public class TrendingWordsInTweets {


    private static Pipeline buildPipeline() {
        Pipeline p = Pipeline.create();

        return p;
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("hazelcast.logging.type", "log4j");
        System.setProperty("hazelcast.partition.count", "1");

        Pipeline pipeline = buildPipeline();

        JetInstance jet = startJet();
        loadStopwordsIntoReplicatedMap(jet);
        TweetPublisher publisher = new TweetPublisher(SAMPLES_HOME + "/books", jet.getMap(TWEETS));
        publisher.start();
        TopListGui gui = new TopListGui(jet.getMap(TOP_LIST));
        try {
            Job job = jet.newJob(pipeline);
            publisher.generateEvents(120);
            Thread.sleep(1000);
            job.cancel();
        } finally {
            publisher.shutdown();
            gui.shutdown();
            Jet.shutdownAll();
        }
    }

    private static JetInstance startJet() {
        JetConfig cfg = new JetConfig();
        cfg.getHazelcastConfig().getMapEventJournalConfig(TWEETS).setEnabled(true);
        return Jet.newJetInstance(cfg);
    }

}

