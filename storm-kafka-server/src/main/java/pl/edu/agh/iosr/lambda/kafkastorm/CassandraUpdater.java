package pl.edu.agh.iosr.lambda.kafkastorm;

import java.io.Serializable;

import org.apache.commons.logging.impl.Log4JLogger;

import storm.trident.state.OpaqueValue;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Fields;

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
	private Fields fields;
	private Fields groupBy;
    
    public CassandraUpdater(String keySpace, String table, Fields fields) {
    	this.keyspace = keySpace;
    	this.table = table;
    	this.fields = fields;
    }
    
    public CassandraUpdater(String keySpace, String table, Fields fields, Fields groupBy) {
    	this.keyspace = keySpace;
    	this.table = table;
    	this.fields = fields;
    	this.groupBy = groupBy;
    }

    @Override
    public Statement map(TridentTuple tuple) {
    	Insert statement = QueryBuilder.insertInto(keyspace, table);
    	
		for(String field : fields)
    		statement.value(field, tuple.getValueByField(field));
	    
	    log.debug("map(): " + statement.getQueryString());
		
        return statement;
    }

    @Override
	public Statement retrieve(TridentTuple tuple) {
	    Selection selection = QueryBuilder.select();
	    
	    for(String field : tuple.getFields())
	    	selection.column(field);
	    
	    Select statement = selection.from(keyspace, table);
	    
	    for(String group : groupBy)
	    	statement.where(QueryBuilder.eq(group, tuple.getValueByField(group)));
	    
	    log.debug("retrieve(): " + statement.getQueryString());
	    
	    return statement;
	}

    public Statement map(TridentTuple tuple, OpaqueValue value) {
    	//TODO ustalic sposob zapisu do bazy
    	
    	Insert statement = QueryBuilder.insertInto(keyspace, table);
    	statement.value(tuple.getFields().get(0), value.getCurr());
	    
	    log.debug("map(): " + statement.getQueryString());
		
        return statement;
    }

	@Override
	public OpaqueValue getValue(Row row) {
		return null;
	}
}