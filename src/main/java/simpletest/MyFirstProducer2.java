package simpletest;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

public class MyFirstProducer2 {

	public static void main(String[] args) {
		
		//创建properties对象  
		Properties kafkaProps=new Properties();
		kafkaProps.put("bootstrap.servers", "oym.com:9092,oym2.com:9092,oym3.com:9092");
		kafkaProps.put("key.serializer", StringSerializer.class.getName());
		kafkaProps.put("value.serializer", StringSerializer.class.getName());
		//kafkaProps.put("partitioner.class", "com.cnw.kafka.producer.MyPartitioner");
		
		//创建KafkaProducer,用于与kafka集群交互，发送消息
		KafkaProducer<String, String> producer= new KafkaProducer<String, String>(kafkaProps);
		
		//创建消息
		ProducerRecord<String, String> record=new ProducerRecord<String, String>("firstTopic","keyvalue", "苗苗最帅");
		//发送消息1
		try {
			/**
			 * Fire-and-forget----此方法用来发送消息到broker，不关注消息是否成功到达。大部分情况下，
			 * 消息会成功到达broker，因为kafka是高可用的，producer会自动重试发送。但是，还是会有消息丢失的情况；
			 */
			producer.send(record);
			producer.close();//flush
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//消息发送2
		/**
		 * Synchronous Send(同步发送)---发送一个消息，send()方法返回一个Future对象，
		 * 使用此对象的get()阻塞方法可以查看send()方法是否执行成功。
		 * 同步方法，会降低消息发送的速度 吞吐量
		 */
		try {
			Future<RecordMetadata> future=producer.send(record);
			RecordMetadata rmd=future.get();
			long offset=rmd.offset();
			int partition=rmd.partition();
			String topic=rmd.topic();
			System.out.println("topic:"+topic+",partition:"+partition+",offset:"+offset);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//消息发送3 回调函数
//		producer.send(record, new MyCallback());
//		producer.close();
		
		//消息发送4多线程发送
		
		
		//自定义分区器
//		ProducerRecord<String, String> record2=new ProducerRecord<String, String>("cnwTopic","1", "hello kafka2 MyCallback");
//		ProducerRecord<String, String> record3=new ProducerRecord<String, String>("cnwTopic","keyvalue", "hello kafka2 MyCallback");
//		producer.send(record2, new MyCallback());
//		producer.send(record3, new MyCallback());
//		producer.close();
		
	}

}
