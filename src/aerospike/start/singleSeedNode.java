package aerospike.start;

import java.util.Map;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.WritePolicy;

public class singleSeedNode {
	
	AerospikeClient client;
	WritePolicy wrpolicy;
	String namespace="test";
	singleSeedNode(String serverAdd,int port) {
		this.client = new AerospikeClient(serverAdd, port);
		this.wrpolicy= new WritePolicy();
		this.wrpolicy.timeout = 50;  // 50 millisecond timeout.
	}
	
	public void writeSingleValue(String binName,String binValue,String setName,String keyName) {
		Key key=new Key(namespace,setName,keyName);
		Bin bin=new Bin(binName,binValue);
		client.put(wrpolicy, key, bin);
		
	}
	public Record readAllBins(Key key) {
		Record rec = client.get(wrpolicy, key);
		return rec;
	}
	public void deleteRecord(String setName,String keyName) {
		Key key = new Key(namespace,setName,keyName);
		client.delete(wrpolicy, key);
	}
	public static void main(String args[]) {
		
		String server="127.0.0.1";
		int port=3000;
		singleSeedNode testObj=new singleSeedNode(server,port);
		testObj.writeSingleValue("myBin", "myBinVlue","mySet", "myKey");
		Key key=new Key(testObj.namespace,"mySet","myKey");
		Record rec=testObj.readAllBins(key);
		
		if (rec != null) {
           		 System.out.println("Found record: Expiration=" + rec.expiration + " Generation=" + rec.generation);
           		 for (Map.Entry<String,Object> entry : rec.bins.entrySet()) {
               			 System.out.println("Name=" + entry.getKey() + " Value=" + entry.getValue());
           		 }

       	 	 }
	
           	 testObj.client.close();
		
	}
}
