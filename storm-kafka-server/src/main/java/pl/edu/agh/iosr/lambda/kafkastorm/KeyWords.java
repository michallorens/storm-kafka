package pl.edu.agh.iosr.lambda.kafkastorm;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

public class KeyWords {

	public static final String SELECT = "select";
	public static final String WHERE = "where";
	public static final String GROUP = "group";
	public static final String BY = "by";

	public static List<String> getFields() {
		List<String> list = new ArrayList<String>();
		
		Field[] fields = KeyWords.class.getDeclaredFields();
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
