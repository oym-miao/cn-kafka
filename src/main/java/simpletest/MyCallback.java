package simpletest;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

public class MyCallback implements Callback {

	public void onCompletion(RecordMetadata metadata, Exception exception) {
		if(exception!=null){
			//异常业务处理
			exception.printStackTrace();
		}
		else{
			long offset=metadata.offset();
			int partition=metadata.partition();
			String topic=metadata.topic();
			System.out.println("topic:"+topic+",partition:"+partition+",offset:"+offset);
		}
	}
}
