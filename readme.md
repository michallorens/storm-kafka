<h1>storm-kafka</h1>
<h2>Integracja z systemem kolejkowym Apache Kafka</h2>
Storm skonfigurowany jest tak, aby nas�uchiwa� przychodz�cych krotek `TridentTuple` z zadanego w�tku systemu kolejkowego - `topicName`. Krotki te tworz� nastepnie strumie� przekazywany do kolejnego elementu w topologii.
```java
ZkHosts zkHosts = new ZkHosts("localhost");
TridentKafkaConfig spoutConfig = new TridentKafkaConfig(zkHosts, topicName);
spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
OpaqueTridentKafkaSpout kafkaSpout = new OpaqueTridentKafkaSpout(spoutConfig);
...
Stream inputStream = topology.newStream(kafkaSpoutName, spout);
```
<h2>Integracja z Cassandra</h2>
Konfiguracja po��czenia z Cassandra:
```java
final Config configuration = new Config();
configuration.put(MapConfiguredCqlClientFactory.TRIDENT_CASSANDRA_CQL_HOSTS, "localhost");
```
Przekazanie przychodz�cych krotek do Cassandry:
```java
UpdateMapper mapper = new UpdateMapper(keySpaceName, tableName, idColumnName, valColumnName);
inputStream.partitionPersist(new CassandraCqlStateFactory(ConsistencyLevel.ONE), new Fields(idColumnName, valColumnName), new CassandraCqlStateUpdater(mapper));
inputStream.each(new Fields(idColumnName, valColumnName), new Debug());
```
Tworzymy klas� `UpdateMapper` odpowiedzialn� za mapowanie krotek na rekordy w Cassandrze i rejestrujemy ja jako `CassandraCqlStateUpdater` na strumieniu z systemu kolejkowego. `UpdateMapper` s�u�y do wykonywania okre�lonego przez nas zapytania CQL:
```java
public Statement map(TridentTuple tuple) {
	SimpleStatement statement = new SimpleStatement(cqlStatement, (Object[]) fields);
	statement.setKeyspace(keySpace);
    return statement;
}
```
<h3>Konfiguracja operacji na Cassandrze</h3>
Okre�lenie odpowiednich przestrzeni, tabeli i kolumn odbywa si� poprzez argumenty wywo�ania:
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
Tworzenie nowego w�tku:
```
bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic sentence-spout --partitions 3 --replication-factor 1
```
Producent i konsument systemu kolejkowego w razie konieczno�ci test�w:
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
bin/storm jar ../storm-kafka-server-1.0-SNAPSHOT-jar-with-dependencies.jar pl.edu.agh.iosr.lambda.kafkastorm.KafkaStormTopology -remote
```
Wszystkie pliki konfiguracyjne konieczne do uruchomienia Storma, Kafki i Zookeepera za��czone s� w katalogu `config/`.
