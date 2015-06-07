package pl.edu.agh.iosr.lambda.kafkastorm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.impl.Log4JLogger;

import com.datastax.driver.core.ConsistencyLevel;
import com.hmsonline.trident.cql.CassandraCqlMapState;
import com.hmsonline.trident.cql.CassandraCqlStateFactory;
import com.hmsonline.trident.cql.CassandraCqlStateUpdater;

import backtype.storm.tuple.Fields;
import storm.trident.Stream;
import storm.trident.fluent.GroupedStream;
import storm.trident.operation.CombinerAggregator;
import storm.trident.operation.builtin.*;

public class SqlMapper {
	private static final List<String> keywords = Arrays.asList("select", "from", "where", "group", "by", "order");
	private static final List<String> aggregates = Arrays.asList("count", "sum", "avg");
    
    private static final Log4JLogger log = new Log4JLogger(SqlMapper.class.getName());
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void buildTopologyFromSql(String sql, Stream tridentStream, String keySpaceName, String tableName) {
		List<List<String>> queries = processSqlString(sql);
		
		Stream splitStream;
		
		for(List<String> query : queries) {
			log.info("Parsowanie zapytania SQL, utworzenie nowej gałęzi w topologii");
			splitStream = tridentStream;
			
			if(query.contains("where"))
				splitStream = where(splitStream, sublist(query, "where"));
			
			if(query.contains("group") && query.contains("by")) {
				GroupedStream groupedStream = groupBy(splitStream, sublist(query, "by"));
	
				//TODO sprawdzić czy można uruchomić wiele funkcji agregujących w jednym zapytaniu
				if(query.contains("count"))
					groupedStream = aggregate(groupedStream, query, keySpaceName, tableName, "count", new Count());
				if(query.contains("sum"))
					groupedStream = aggregate(groupedStream, query, keySpaceName, tableName, "sum", new Sum());
				if(query.contains("avg"))
					groupedStream = aggregate(groupedStream, query, keySpaceName, tableName, "avg", new AverageAggregator());
			} else {
				log.info("Zapisywanie pól wybranych w wyniku zapytania SELECT do cassandry");
				
//				if(Arrays.asList(sublist(query, "select")).contains("*")) {
//			        splitStream.partitionPersist(new CassandraCqlStateFactory(ConsistencyLevel.ONE), new Fields(StockFieldsDescriptor.stockFields),
//			        		new CassandraCqlStateUpdater(new CassandraUpdater(keySpaceName, tableName, new Fields(StockFieldsDescriptor.stockFields))));
//				} else {
		        Fields fields = new Fields(sublist(query, "select"));
		
		        splitStream.partitionPersist(new CassandraCqlStateFactory(ConsistencyLevel.ONE), fields, 
		        		new CassandraCqlStateUpdater(new CassandraUpdater(keySpaceName, tableName, fields)));
			}
		}
	}
	
	public static List<List<String>> processSqlString(String sql) {
		List<List<String>> result = new ArrayList<List<String>>();
		List<String> queries = new ArrayList<String>(Arrays.asList(sql.split(";")));
		for(String str : queries) {
			if(str.equals(""))
				continue;
			List<String> query = new ArrayList<String>(Arrays.asList(str.replaceAll("[,()]+", " ").split(" ")));
			while(query.contains(""))
				query.remove("");
			result.add(query);
		}
		
		return result;
	}
	
	public static String[] sublist(List<String> list, String from) {
		int i = list.indexOf(from) + 1;
		while(i < list.size()) {
			if(keywords.contains(list.get(i)))
				break;
			i++;
		}
		
		list = list.subList(list.indexOf(from) + 1, i);
		return list.toArray(new String[list.size()]);
	}
	
	public static String[] getFields(String[] array) {
		List<String> fields = new ArrayList<String>();
		
		for(String str : array) {
			if(!str.matches(".*[0123456789=<>\"]+.*"))
				fields.add(str);
		}
		
		return fields.toArray(new String[fields.size()]);
	}
	
	public static List<String> removeAggregates(List<String> query) {
		List<String> result = new ArrayList<String>(query);
		
		for(String agg : aggregates) {
			while(result.contains(agg))
				result.remove(agg);
		}
		
		return result;
	}
	
	private static Stream where(Stream tridentStream, String[] where) {
		log.info("Dodanie warunku where: " + where.toString());
		return tridentStream.each(new Fields(getFields(where)), new WhereFilter(where));
	}
	
//	private static Stream select(Stream tridentStream, String[] select) {
//		return tridentStream.project(new Fields(select));
//	}
	
	private static GroupedStream groupBy(Stream tridentStream, String[] groupBy) {
		log.info("Podział strumienia GROUP BY po polach: " + groupBy.toString());
		return tridentStream.groupBy(new Fields(groupBy));
	}
	
	private static GroupedStream aggregate(GroupedStream groupedStream, List<String> query, String keySpaceName, String tableName, String aggregate, @SuppressWarnings("rawtypes") CombinerAggregator aggregator) {
		log.info("Uruchomienie funkcji agregującej " + aggregate + "() na podzielonym strumieniu");
		
		Fields aggregateField = new Fields(aggregate + "_" + query.get(query.indexOf(aggregate) + 1));
		
//		if(Arrays.asList(sublist(query, "select")).contains("*")) {
//			
//			groupedStream.persistentAggregate(CassandraCqlMapState.opaque(
//					new CassandraUpdater(keySpaceName, tableName, new Fields(StockFieldsDescriptor.stockFields), new Fields(sublist(query, "by")))), 
//					aggregator, aggregateField);
//		} else {
		query = removeAggregates(query);
		Fields fields = new Fields(sublist(query, "select"));
		
		groupedStream.persistentAggregate(CassandraCqlMapState.opaque(
				new CassandraUpdater(keySpaceName, tableName, fields, new Fields(sublist(query, "by")))), 
				fields,	aggregator, aggregateField);

		return groupedStream;
	}
}
