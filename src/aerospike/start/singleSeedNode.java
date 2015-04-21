package aerospike.start;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.WritePolicy;

public class singleSeedNode {
	
	AerospikeClient client;
	WritePolicy wrpolicy;
	String namespace=null;
	singleSeedNode(String serverAdd,int port) {
		this.client = new AerospikeClient(serverAdd, port);
		this.wrpolicy= new WritePolicy();
		this.wrpolicy.timeout = 50;  // 50 millisecond timeout.
	}
	
	public void writeSingleValue(String binName,String binValue,String setName,String keyName) {
		if(namespace.isEmpty()) {
			namespace="test";
		}
		Key key=new Key(namespace,setName,keyName);
		Bin bin=new Bin(binName,binValue);
		client.put(wrpolicy, key, bin);
		
	}
	public Record readAllBins(Key key) {
		Record rec = client.get(wrpolicy, key);
		return rec;
	}
	public void deleteRecord(String setName,String keyName) {
		if(namespace.isEmpty()) {
			namespace="test";
		}
		Key key = new Key(namespace,setName,keyName);
		client.delete(wrpolicy, key);
	}
	public static void main(String args[]) {
	 
		
	}
}
