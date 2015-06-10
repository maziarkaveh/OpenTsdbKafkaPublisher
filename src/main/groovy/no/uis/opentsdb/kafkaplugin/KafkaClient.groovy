package no.uis.opentsdb.kafkaplugin

import groovy.json.JsonOutput
import kafka.javaapi.producer.Producer
import kafka.producer.KeyedMessage
import kafka.producer.ProducerConfig

class KafkaClient {
    Producer producer

    void send(Map data) {
        def msg = new KeyedMessage<String, String>(data.metric, 'spark', JsonOutput.toJson(data))
        producer.send(msg)

    }

    KafkaClient(String brokerList, String zookeeperConnect) {
        this.producer = producer
        def props = new Properties()
        props.put('zookeeper.connect', zookeeperConnect);
        props.put('serializer.class', 'kafka.serializer.StringEncoder')
        props.put('metadata.broker.list', brokerList);

        def config = new ProducerConfig(props)
        producer = new Producer<String, String>(config)
    }
}
