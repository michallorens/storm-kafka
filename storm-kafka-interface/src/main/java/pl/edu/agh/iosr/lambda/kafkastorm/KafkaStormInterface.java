package pl.edu.agh.iosr.lambda.kafkastorm;

import java.rmi.Remote;
import java.rmi.RemoteException;

import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
 
public interface KafkaStormInterface extends Remote {
    public void createTopology(String zkHost, String cqlHost, String topologyName, String topicName,
			String keySpaceName, String tableName, String kafkaSpoutName, String sqlStatement, Integer numWorkers) 
					throws RemoteException, AlreadyAliveException, InvalidTopologyException;
}

