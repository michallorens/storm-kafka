package pl.edu.agh.iosr.lambda.kafkastorm;

import org.apache.commons.logging.impl.Log4JLogger;

import storm.trident.operation.ReducerAggregator;
import storm.trident.tuple.TridentTuple;

public class AverageAggregator implements ReducerAggregator<Double> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

    private static final Log4JLogger log = new Log4JLogger(AverageAggregator.class.getName());
	
	@Override
	public Double init() {
		return 0.;
	}

	@Override
	public Double reduce(Double curr, TridentTuple tuple) {
		log.info("avg(): " + curr + ", " + tuple.getValueByField("sum") + ", " + tuple.getValueByField("count"));
		
		return (curr * (Double.parseDouble((String) tuple.getValueByField("count")) - 1) +
				Double.parseDouble((String) tuple.getValueByField("sum"))) /
				Double.parseDouble((String) tuple.getValueByField("count"));
	}
}
