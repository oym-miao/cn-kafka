package simpletest;

import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.record.InvalidRecordException;
import org.apache.kafka.common.utils.Utils;

public class MyPartitioner implements Partitioner {

	public void configure(Map<String, ?> configs) {
		// TODO Auto-generated method stub

	}

	public int partition(String topic, Object key, byte[] keyBytes,Object value, byte[] valueBytes, Cluster cluster) {
//		System.out.println("MyPartitioner running。。。。。。。。");
		//如果key为1，发送到partition：1分区。如果key为其他值，使用默认分区方法。
		//获取指定topic的partition列表
		List<PartitionInfo>  partitioins=cluster.partitionsForTopic(topic);
		int numPartitions=partitioins.size();
		if(key.equals("1")){
			System.out.println("MyPartitioner running。。。。。。。。"+key);
			return 1;//如果key为1，发送到partition：1分区
		}
		//如果key为其他值，使用默认分区方法。
		return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
	}

	
	public void close() {

	}

}
