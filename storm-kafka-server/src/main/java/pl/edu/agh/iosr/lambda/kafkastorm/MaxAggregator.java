package pl.edu.agh.iosr.lambda.kafkastorm;

import storm.trident.operation.CombinerAggregator;
import storm.trident.tuple.TridentTuple;

public class MaxAggregator implements CombinerAggregator<Double> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public Double init(TridentTuple tuple) {
		return Double.parseDouble((String) tuple.getValue(0));
	}

	@Override
	public Double combine(Double val1, Double val2) {
		return (val1 > val2) ? val1 : val2;
	}

	@Override
	public Double zero() {
		return 0.;
	}

}
