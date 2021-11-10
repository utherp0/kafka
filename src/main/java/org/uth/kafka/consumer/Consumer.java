package org.uth.kafka.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class Consumer
{
  public static void main( String args[] )
  {
    if( args.length != 2 )
    {
      System.out.println( "Usage: java org.uth.kafka.consumer.Consumer bootstrap-server topic");
      System.exit(0);
    }

    String bootstrap = args[0];
    String topic = args[1];

    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", bootstrap );
    properties.setProperty("group.id", "uth");
    //properties.setProperty("enable.auto.commit", "true");
    //properties.setProperty("auto.commit.interval.ms", "1000");
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

    KafkaConsumer<String, String> consumer = new KafkaConsumer<>( properties );

    consumer.subscribe(Arrays.asList( topic ));

    while( true )
    {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

      for (ConsumerRecord<String, String> record : records)
      {
        System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
      }
    }
  }
}
