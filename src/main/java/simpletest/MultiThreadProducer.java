package simpletest;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class MultiThreadProducer extends Thread {

	private KafkaProducer<String, String> producer;
	private String topic;
	
	public MultiThreadProducer(String topicName) {
		//创建properties对象  
		Properties kafkaProps=new Properties();
		kafkaProps.put("bootstrap.servers", "bigdata01:9092,bigdata02:9092,bigdata03:9092");
		kafkaProps.put("key.serializer", StringSerializer.class.getName());
		kafkaProps.put("value.serializer", StringSerializer.class.getName());
		//创建KafkaProducer,用于与kafka集群交互，发送消息
		producer= new KafkaProducer<String, String>(kafkaProps);
		topic=topicName;
	}
	
	@Override
	public void run() {
		int messageNo=0;
		while(messageNo<3000){
			String messageContent="Message_"+messageNo;
			//创建消息
			ProducerRecord<String, String> record=new ProducerRecord<String, String>(topic,messageNo+"", messageContent);
			producer.send(record, new MyCallback());
			messageNo++;
		}
		producer.flush();
	}
	
	public static void main(String[] args) {
		ExecutorService es=Executors.newFixedThreadPool(3);
		for(int i=0;i<10;i++){
			es.execute(new MultiThreadProducer("cnwTopic"));
		}
		es.shutdown();
	}
}
