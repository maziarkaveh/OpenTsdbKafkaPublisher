package no.uis.opentsdb.kafkaplugin

import com.stumbleupon.async.Deferred
import net.opentsdb.core.TSDB
import net.opentsdb.meta.Annotation
import net.opentsdb.stats.StatsCollector
import net.opentsdb.tsd.RTPublisher
import org.slf4j.LoggerFactory

class KafkaPublisher extends RTPublisher {

    static final LOG = LoggerFactory.getLogger(KafkaPublisher)
    KafkaClient  kafkaClient

    void initialize(final TSDB tsdb) {
        def brokerList = tsdb.config.getString('tsd.plugin.kafka.broker.list')
        def zookeeperConnect = tsdb.config.getString('tsd.plugin.kafka.zookeeper.connect')
        if (!brokerList) {
            LOG.error('tsd.plugin.kafka.broker.list is not set in opentsdb.conf')
        }
        if (!zookeeperConnect) {
            LOG.error('tsd.plugin.kafka.zookeeper.connect is not set in opentsdb.conf')
        }
        kafkaClient = new KafkaClient(brokerList, zookeeperConnect)
        LOG.info('init initialize')
    }

    @Override
    Deferred<Object> publishAnnotation(Annotation annotation) {
        LOG.info('init publishAnnotation')
        new Deferred<Object>()
    }

    Deferred<Object> shutdown() {
        LOG.info('init shutdown')
        new Deferred<Object>()
    }

    String version() {
        LOG.info('init version')
        '0.1.0'
    }

    void collectStats(final StatsCollector collector) {
        LOG.info('init collectStats')
    }

    Deferred<Object> publishDataPoint(final String metric,
                                      final long timestamp, final long value, final Map<String, String> tags,
                                      final byte[] tsuid) {
        publishDataPoint(metric, timestamp, value, tags, tsuid)
    }

    Deferred<Object> publishDataPoint(final String metric,
                                      final long timestamp, final double value, final Map<String, String> tags,
                                      final byte[] tsuid) {
        LOG.info("publishDataPoint publishDataPoint $timestamp $metric $value $tags $tsuid")
        kafkaClient.send([metric: metric, timestamp: timestamp, value: value, tags: tags, tsuid: tsuid])
        new Deferred<Object>()
    }
}
