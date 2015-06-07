package pl.edu.agh.iosr.lambda.kafkastorm;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import pl.edu.agh.iosr.lambda.dropwizard.config.StockFieldsDescriptor;
import pl.edu.agh.iosr.lambda.dropwizard.config.iface.FieldsDescriptor;
import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

public class DeserializationFunction implements Function {

    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		// TODO Auto-generated method stub

	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		FieldsDescriptor fieldsDescriptor = new StockFieldsDescriptor();
		
        String jsonBody = tuple.getString(0);
        System.out.println("\n\n\n\n" + jsonBody + "\n\n\n\n");
        Map<String,String> data = fieldsDescriptor.extractMapFromJson(jsonBody);
        List<Object> valuesList = new ArrayList<Object>();

        for(String field : fieldsDescriptor.getAllFields()){
            valuesList.add(data.get(field));
        }

        //to niby jest Object, ale w praktyce są tam stringi, po prostu emmiter nie przyjął
        collector.emit(valuesList);

	}

}
