package pl.edu.agh.iosr.lambda.kafkastorm;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;


public class Aggregates {

	public static final String COUNT = "count";
	public static final String SUM = "sum";
	public static final String MIN = "min";
	public static final String MAX = "max";
	public static final String AVG = "avg";

	public static List<String> getFields() {
		List<String> list = new ArrayList<String>();
		
		Field[] fields = Aggregates.class.getDeclaredFields();
		for (Field f : fields) {
		    if (Modifier.isStatic(f.getModifiers())) {
		        try {
					list.add((String) f.get(null));
				} catch (IllegalArgumentException e) {
					e.printStackTrace();
				} catch (IllegalAccessException e) {
					e.printStackTrace();
				}
		    } 
		}
		
		return list;
	}

}
