<h1>storm-kafka</h1>
<h2>Integracja z systemem kolejkowym Apache Kafka</h2>
Storm skonfigurowany jest tak, aby nasluchiwal przychodzacych krotek `TridentTuple` z zadanego watku systemu kolejkowego - `topicName`. Krotki te tworza nastepnie strumien przekazywany do kolejnego elementu w topologii.
```java
ZkHosts zkHosts = new ZkHosts("localhost");
TridentKafkaConfig spoutConfig = new TridentKafkaConfig(zkHosts, topicName);
spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
OpaqueTridentKafkaSpout kafkaSpout = new OpaqueTridentKafkaSpout(spoutConfig);
...
Stream inputStream = topology.newStream(kafkaSpoutName, spout);
```
<h2>Integracja z Cassandra</h2>
Konfiguracja polaczenia z Cassandra:
```java
final Config configuration = new Config();
configuration.put(MapConfiguredCqlClientFactory.TRIDENT_CASSANDRA_CQL_HOSTS, "localhost");
```
Przekazanie przychodzacych krotek do Cassandry:
```java
UpdateMapper mapper = new UpdateMapper(keySpaceName, tableName, idColumnName, valColumnName);
inputStream.partitionPersist(new CassandraCqlStateFactory(ConsistencyLevel.ONE), new Fields(idColumnName, valColumnName), new CassandraCqlStateUpdater(mapper));
inputStream.each(new Fields(idColumnName, valColumnName), new Debug());
```
Tworzymy klase `UpdateMapper` odpowiedzialna za mapowanie krotek na rekordy w Cassandrze i rejestrujemy ja jako `CassandraCqlStateUpdater` na strumieniu z systemu kolejkowego. `UpdateMapper` sluzy do wykonywania okreslonego przez nas zapytania CQL:
```java
public Statement map(TridentTuple tuple) {
	SimpleStatement statement = new SimpleStatement(cqlStatement, (Object[]) fields);
	statement.setKeyspace(keySpace);
    return statement;
}
```
<h3>Konfiguracja operacji na Cassandrze</h3>
Okreslenie odpowiednich przestrzeni, tabeli, kolumn odbywa sie poprzez argumenty wywolania:
```
 -cqlhost VAL    : Cassandra host address (default: localhost)
 -kafkaspout VAL : Kafka spout name (default: kafka-spout)
 -keySpace VAL   : Cassandra keyspace name (default: demo)
 -remote         : Run as RMI server (default: false)
 -sql VAL        : SQL statement/query (default: insert into demo.stormcf
                   (word, count) values (?, ?))
 -table VAL      : Storm topology name (default: stormcf)
 -topic VAL      : Kafka topic name (default: tuple)
 -topology VAL   : Storm topology name (default: test-topology)
 -workers N      : Number of worker threads (default: 3)
 -zkhost VAL     : ZooKeeper host address (default: localhost)
```
<h2>Uruchamianie aplikacji</h2>
<h3>Uruchomienie systemu kolejkowego</h3>
```
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server0.properties
```
Tworzenie nowego watku:
```
bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic sentence-spout --partitions 3 --replication-factor 1
```
Producent i konsument systemu kolejkowego w razie koniecznosci testow:
```
bin/kafka-console-producer.sh --broker-list localhost:9092 --sync --topic sentence-spout
bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic sentence-spout --from-beginning
```
<h3>Uruchomienie Storma na klastrze</h3>
```
bin/storm nimbus
bin/storm supervisor
bin/storm ui
```
Uruchomienie topologii:
```
java -Dstorm.jar=apache-storm-0.9.4/bin/storm -jar storm-kafka-server-1.0-SNAPSHOT-jar-with-dependencies.jar -remote
```
lub:
```
bin/storm jar ../storm-kafka-server-1.0-SNAPSHOT-jar-with-dependencies.jar pl.edu.agh.iosr.lambda.kafkastorm.KafkaStormTopology [argumenty]
```
