package webinar;

import com.hazelcast.config.Config;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JetConfig;

import java.util.PriorityQueue;

public class JetProcess {
    public static final int PARTITION_COUNT = 271;

    public static void main(String[] args) {
        startJet();
    }

    static JetInstance startJet() {
        System.setProperty("hazelcast.logging.type", "log4j");
        System.setProperty("hazelcast.partition.count", String.valueOf(PARTITION_COUNT));

        JetConfig cfg = new JetConfig();
        Config hzCfg = cfg.getHazelcastConfig();
        hzCfg.getSerializationConfig().addSerializerConfig(new SerializerConfig()
                .setImplementation(new PriorityQueueSerializer())
                .setTypeClass(PriorityQueue.class));
        hzCfg.getMapEventJournalConfig(TrendingWordsInTweets.TWEETS).setEnabled(true);
        cfg.getInstanceConfig().setScaleUpDelayMillis(1000);
        return Jet.newJetInstance(cfg);
    }
}
