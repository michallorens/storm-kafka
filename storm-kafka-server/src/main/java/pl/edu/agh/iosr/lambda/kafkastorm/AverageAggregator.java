package pl.edu.agh.iosr.lambda.kafkastorm;

import storm.trident.operation.CombinerAggregator;
import storm.trident.tuple.TridentTuple;

public class AverageAggregator implements CombinerAggregator<Number> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	int count = 0;
	double sum = 0;

	@Override
	public Double init(final TridentTuple tuple) {
	    this.count++;
	    if (!(tuple.getValue(0) instanceof Double)) {
	
	        double d = ((Number) tuple.getValue(0)).doubleValue();
	
	        this.sum += d;
	
	        return d;
	    }
	
	    this.sum += (Double) tuple.getValue(0);
	    return (Double) tuple.getValue(0);
	
	}

	@Override
	public Double combine(final Number val1, final Number val2) {
	    return this.sum / this.count;
	
	}
	
	@Override
	public Double zero() {
	    this.sum = 0;
	    this.count = 0;
	    return 0D;
	}
}
