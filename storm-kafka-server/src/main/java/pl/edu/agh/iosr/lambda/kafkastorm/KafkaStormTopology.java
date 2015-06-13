package pl.edu.agh.iosr.lambda.kafkastorm;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;

import org.apache.commons.logging.impl.Log4JLogger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import pl.edu.agh.iosr.lambda.dropwizard.config.StockFieldsDescriptor;

import com.hmsonline.trident.cql.MapConfiguredCqlClientFactory;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Debug;

public class KafkaStormTopology extends UnicastRemoteObject implements KafkaStormInterface {
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private static final Log4JLogger log = new Log4JLogger(KafkaStormTopology.class.getName());
//	private static final String zkHostPort = "172.17.84.117:2181";
    
    @Option(name="-remote", usage="Run as RMI server")
	private boolean remote = false;

	@Option(name="-zkhost", usage="ZooKeeper host address")
	private String zkHost = "localhost";
    
    @Option(name="-cqlhost", usage="Cassandra host address")
	private String cqlHost = "localhost";

    @Option(name="-topology", usage="Storm topology name")
	private String topologyName = "test-topology";
    
    @Option(name="-topic", usage="Kafka topic name")
    private String topicName = "tuple";
	
    @Option(name="-keySpace", usage="Cassandra keyspace name")
	private String keySpaceName = "demo";

    @Option(name="-table", usage="Storm topology name")
	private String tableName = "stormcf";

    @Option(name="-kafkaspout", usage="Kafka spout name")
	private String kafkaSpoutName = "kafka-spout";
    
    @Option(name="-sql", usage="SQL statement/query")
    private String sqlStatement = "insert into demo.stormcf (word, count) values (?, ?)";
    
    @Option(name="-workers", usage="Number of worker threads")
	private Integer numWorkers = 3;
    
    public boolean isRemote() {
		return remote;
	}

	private StormTopology buildTopology() {
        log.info("Budowanie topologii...");
        TridentTopology topology = new TridentTopology();
        
        ZkHosts zkHosts = new ZkHosts(zkHost);
        TridentKafkaConfig spoutConfig = new TridentKafkaConfig(zkHosts, topicName);
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        OpaqueTridentKafkaSpout kafkaSpout = new OpaqueTridentKafkaSpout(spoutConfig);
        
        log.info("Subskrypcja na strumień danych z kafki");
        Stream tridentStream = topology.newStream(kafkaSpoutName, kafkaSpout);
        tridentStream = tridentStream.each(new Fields("str"), new DeserializationFunction(), new Fields(new StockFieldsDescriptor().getAllFields()));
        log.info("Ustawianie debugowania pól strumienia danych przychodzących z kafki");
        tridentStream.each(new Fields(new StockFieldsDescriptor().getAllFields()), new Debug());
        log.info("Odrzucanie zbędnych pól");
        tridentStream = tridentStream.project(new Fields(new StockFieldsDescriptor().getAllFields()));
        SqlMapper.buildTopologyFromSql(sqlStatement, tridentStream, keySpaceName, tableName);
        return topology.build();
    }
	
	private void configureTopology() throws AlreadyAliveException, InvalidTopologyException {
		final Config configuration = new Config();
		log.info("Konfiguracja połączenia z Cassandrą");
        configuration.put(MapConfiguredCqlClientFactory.TRIDENT_CASSANDRA_CQL_HOSTS, cqlHost);
        
        log.info("Zatwierdzanie topologii");
        
    	configuration.setNumWorkers(numWorkers);
        StormSubmitter.submitTopology(topologyName, configuration, buildTopology());
        
        log.info("Złożono topologię na klastrze");
	}
	
	@Override
	public void createTopology(String zkHost, String cqlHost, String topologyName, String topicName,
			String keySpaceName, String tableName, String kafkaSpoutName, String sqlStatement, Integer numWorkers) 
					throws AlreadyAliveException, InvalidTopologyException {
		if(zkHost != null)
			this.zkHost = zkHost;
		if(cqlHost != null)
			this.cqlHost = cqlHost;
		if(topologyName != null)
			this.topologyName = topologyName;
		if(topicName != null)
			this.topicName = topicName;
		if(keySpaceName != null)
			this.keySpaceName = keySpaceName;
		if(tableName != null)
			this.tableName = tableName;
		if(kafkaSpoutName != null)
			this.kafkaSpoutName = kafkaSpoutName;
		if(sqlStatement != null)
			this.sqlStatement = sqlStatement;
		if(numWorkers != null)
			this.numWorkers = numWorkers;
		
		configureTopology();
	}
	
	private KafkaStormTopology() throws RemoteException {
		super(0);
	}
	
	public static void main(String[] args) throws RemoteException, InterruptedException, AlreadyAliveException, InvalidTopologyException {
		KafkaStormTopology kafkaStormTopology = new KafkaStormTopology();
		CmdLineParser parser = new CmdLineParser(kafkaStormTopology);
		
	    try {
	        parser.parseArgument(args);
		} catch(CmdLineException e) {
		    System.err.println(e.getMessage());
		    parser.printUsage(System.err);
		}

	    if(kafkaStormTopology.isRemote()) {
	        log.info("Uruchmianie serwera RMI");
	 
	        try {
	            LocateRegistry.createRegistry(1099).rebind("KafkaStormTopology", kafkaStormTopology); 
	            log.info("Utworzono rejestr java RMI");
//		        Naming.rebind("//localhost/KafkaStormTopology", kafkaStormTopology);
		        log.info("Zbindowano serwer w rejestrze");
	        } catch (RemoteException e) {
	            log.error("Dany rejestr java RMI już istnieje");
	            e.printStackTrace();
//	        } catch (MalformedURLException e) {
//	        	log.error("Błędny URL");
//				e.printStackTrace();
			}
	    } else {
			kafkaStormTopology.configureTopology();
	    }
	}
}
