package hw1;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * A class to perform various aggregations, by accepting one tuple at a time
 * @author Doug Shook
 *
 */
public class Aggregator {
	
	private boolean groupBy;
	private TupleDesc td;
	private AggregateOperator AggOp;
	private Map tuples;
	
	
	public Aggregator(AggregateOperator o, boolean groupBy, TupleDesc td) {
		//your code here
		this.groupBy = groupBy;
		this.td = td;
		this.AggOp = o;
		
		if(td.getType(0) == Type.INT) {
			tuples = new HashMap<Integer, Integer>();
		}
		else {
			if(groupBy) {
				tuples = new HashMap<String, Integer>();
			}
			else {
				if(o==AggregateOperator.COUNT) {
					tuples = new HashMap<String, Integer>();
				}
				else {
					tuples = new HashMap<String, String>();
				}
			}
		}
	}

	/**
	 * Merges the given tuple into the current aggregation
	 * @param t the tuple to be aggregated
	 */
	public void merge(Tuple t) {
		//your code here
		if(!groupBy) {
			if(td.getType(0)==Type.STRING) {
				switch(AggOp) {
					case MAX: 
						if(tuples.get(0) == null) {
							tuples.put(0, ((StringField)t.getField(0)).getValue());
						}
						else {
							if(((StringField)t.getField(0)).getValue().compareTo(tuples.get(0).toString())==1) {
								tuples.put(0, ((StringField)t.getField(0)).getValue());
							}
						}
					
				}
			}
		}
	}
	
	/**
	 * Returns the result of the aggregation
	 * @return a list containing the tuples after aggregation
	 */
	public ArrayList<Tuple> getResults() {
		//your code here
		return null;
	}

}
