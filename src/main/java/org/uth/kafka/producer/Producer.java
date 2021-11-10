package org.uth.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class Producer
{
  public static void main( String[] args )
  {
    if( args.length != 2 )
    {
      System.out.println( "Usage: java org.uth.kafka.producer.Producer bootstrap-server topic");
      System.exit(0);
    }

    String bootstrap = args[0];
    String topic = args[1];

    Properties properties = new Properties();

    properties.put("bootstrap.servers", bootstrap);
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    KafkaProducer producer = new KafkaProducer(properties);

    try
    {
      for( int loop = 0; loop < 10000; loop++ )
      {
        System.out.println( "Sending record " + loop);
        producer.send( new ProducerRecord( topic, "Message " + loop, "Contents of message " + loop ));
      }
    }
    catch( Exception exc )
    {
      System.out.println( "Failed to push messages due to " + exc.toString());
    }
    finally
    {
      producer.close();
    }
  }
}
