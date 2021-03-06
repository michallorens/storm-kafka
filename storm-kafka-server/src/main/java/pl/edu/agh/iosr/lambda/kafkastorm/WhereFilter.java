package pl.edu.agh.iosr.lambda.kafkastorm;

import javax.script.ScriptEngineManager;
import javax.script.ScriptEngine;
import javax.script.ScriptException;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

@SuppressWarnings("restriction")
public class WhereFilter extends BaseFilter {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private String where = "";
	
	public WhereFilter(String[] array) {
		StringBuilder sb = new StringBuilder(this.where);
		
		for(String str : array) {
			sb.append(str);
			sb.append(" ");
		}
		
		this.where = sb.toString();
	}

	@Override
	public boolean isKeep(TridentTuple tuple) {
		ScriptEngineManager mgr = new ScriptEngineManager();
		ScriptEngine engine = mgr.getEngineByName("JavaScript");
		
		try {
			for(String field : tuple.getFields())
				engine.eval(field + " = " + tuple.getValueByField(field).toString());
			
			return (Boolean) engine.eval(where);
		} catch (ScriptException e) {
			e.printStackTrace();
		}
		
		return false;
	}

}
