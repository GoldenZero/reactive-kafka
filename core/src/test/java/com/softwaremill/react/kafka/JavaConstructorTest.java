package com.softwaremill.react.kafka;


import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import kafka.serializer.StringEncoder;
import org.junit.Ignore;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import static junit.framework.Assert.assertNotNull;

public class JavaConstructorTest {

    @Test
    public void javaCanConstructReactiveKafkaWithoutDefaultArgs() {
        final ReactiveKafka reactiveKafka = new ReactiveKafka();
        assertNotNull(reactiveKafka);
    }

    @Test
    @Ignore("Disabled as we need a kafka endpoint - this example is displayed on the README")
    public void javaCanConstructKafkaConsumerAndProducerInJava() {

        String zooKeeperHost = "localhost:2181";
        String brokerList = "localhost:9092";

        ReactiveKafka kafka = new ReactiveKafka();
        ActorSystem system = ActorSystem.create("ReactiveKafka");
        ActorMaterializer materializer = ActorMaterializer.create(system);

        ConsumerProperties<String> cp =
                new PropertiesBuilder.Consumer(brokerList, zooKeeperHost, "topic", "groupId", new StringDecoder(null))
                        .build();

        Publisher<MessageAndMetadata<byte[], String>> publisher = kafka.consume(cp, system);

        ProducerProperties<String> pp = new PropertiesBuilder.Producer(
                brokerList,
                zooKeeperHost,
                "topic",
                new StringEncoder(null)).build();
        Subscriber<String> subscriber = kafka.publish(pp, system);
        //TODO fix this compilation error
        /*
            [error]   required: java.lang.Iterable<O>
            [error]   found: org.reactivestreams.Publisher<kafka.message.MessageAndMetadata<byte[],java.lang.String>>
            [error]   reason: cannot infer type-variable(s) O
            [error]     (argument mismatch; org.reactivestreams.Publisher<kafka.message.MessageAndMetadata<byte[],java.lang.String>> cannot be converted to java.lang.Iterable<O>)
            [error] Source.from
         */
        //Source.from(publisher).map(msg -> msg.message()).to(Sink.fromSubscriber(subscriber)).run(materializer);
    }

}
