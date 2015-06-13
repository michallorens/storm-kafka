package pl.edu.agh.iosr.lambda.kafkastorm;

import java.io.Serializable;

import org.apache.commons.logging.impl.Log4JLogger;

import storm.trident.state.OpaqueValue;
import storm.trident.tuple.TridentTuple;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Selection;
import com.hmsonline.trident.cql.mappers.CqlRowMapper;

@SuppressWarnings("rawtypes")
public class CassandraUpdater implements CqlRowMapper<TridentTuple, OpaqueValue>, Serializable {
    private static final long serialVersionUID = 1L;
    
    private static final Log4JLogger log = new Log4JLogger(CassandraUpdater.class.getName());
	
	private String keyspace;
	private String table;
	private Integer id;
    
    public CassandraUpdater(String keySpace, String table, int id) {
    	this.keyspace = keySpace;
    	this.table = table;
    	this.id = id;
    }

    @Override
    public Statement map(TridentTuple tuple) {
    	StringBuilder sb = new StringBuilder();

		sb.append(tuple.get(0));
		for(int i = 1; i < tuple.getFields().size(); i++) {
    		sb.append(" | ");
    		sb.append(tuple.get(i));
    	}

    	Insert statement = QueryBuilder.insertInto(keyspace, table);
    	statement.value("query_id", id);
    	statement.value("group_by", "");
    	statement.value("result", sb.toString());
	    
	    log.info("map(): " + statement.getQueryString());
		
        return statement;
    }

    @Override
	public Statement retrieve(TridentTuple tuple) {
	    Selection selection = QueryBuilder.select();
	    
//	    for(String field : tuple.getFields())
//	    	selection.column(field);
	    
	    Select statement = selection.from(keyspace, "stormcf");
	    
//	    for(String group : groupBy)
//	    	statement.where(QueryBuilder.eq(group, tuple.getValueByField(group)));
	    
	    log.info("retrieve(): " + statement.getQueryString());
	    
	    return statement;
	}

    public Statement map(TridentTuple tuple, OpaqueValue value) {
    	StringBuilder sb = new StringBuilder();

		sb.append(tuple.get(0));
		for(int i = 1; i < tuple.getFields().size(); i++) {
    		sb.append(" | ");
    		sb.append(tuple.get(i));
    	}
    	
    	log.info("map(): #" + id + " " + sb);
    	
    	Insert statement = QueryBuilder.insertInto(keyspace, table);
    	statement.value("query_id", id);
    	statement.value("group_by", sb.toString());
    	statement.value("result", value.getCurr().toString());
	    
	    log.info("map(): " + statement.getQueryString());
		
        return statement;
    }

	@Override
	public OpaqueValue getValue(Row row) {
		return null;
	}
}