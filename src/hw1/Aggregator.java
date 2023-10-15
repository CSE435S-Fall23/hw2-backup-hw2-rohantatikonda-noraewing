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
	private Map<Field, Integer> counter;
	
	
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
					case AVG: 
						break;
					case SUM:
						break;
					case MAX: 
						if(tuples.get(0) == null) {
							tuples.put(0, ((StringField)t.getField(0)).getValue());
						}
						else {
							if(((StringField)t.getField(0)).getValue().compareTo(tuples.get(0).toString())==1) {
								tuples.put(0, ((StringField)t.getField(0)).getValue());
							}
						}
						break;
					case MIN:
						if(tuples.get(0) == null) {
							tuples.put(0, ((StringField)t.getField(0)).getValue());
						}
						else {
							if(((StringField)t.getField(0)).getValue().compareTo(tuples.get(0).toString())==-1) {
								tuples.put(0, ((StringField)t.getField(0)).getValue());
							}
						}
						break;
					case COUNT:
						if(tuples.get(0) == null) {
							tuples.put(0, 1);
						}
						else {
							tuples.put(0, (int)tuples.get(0)+1);
						}
						break;
				}
			}
			if(td.getType(0) == Type.INT) {
				switch(AggOp) {
					case MAX:
						if(tuples.get(0) == null) {
							tuples.put(0, ((IntField)t.getField(0)).getValue());
						}
						else {
							if((int)tuples.get(0)<((IntField)t.getField(0)).getValue()) {
								tuples.put(0, ((IntField)t.getField(0)).getValue());
							}
						}
						break;
					case MIN:
						if(tuples.get(0) == null) {
							tuples.put(0, ((IntField)t.getField(0)).getValue());
						}
						else {
							if((int)tuples.get(0)>((IntField)t.getField(0)).getValue()) {
								tuples.put(0, ((IntField)t.getField(0)).getValue());
							}
						}
						break;
					case AVG:
						if(tuples.get(0) == null) {
							tuples.put(0, ((IntField)t.getField(0)).getValue());
						}
						else {
							tuples.put(tuples.size(),((IntField)t.getField(0)).getValue());
			        		tuples.put(0, ((((IntField)t.getField(0)).getValue())+(tuples.size()-1*((int)tuples.get(0))))/tuples.size());
						}
						break;
					case COUNT:
						if(tuples.get(0) == null) {
							tuples.put(0, 1);
						}
						else {
							tuples.put(0,(int)tuples.get(0)+1);
						}
						break;
					case SUM:
						if(tuples.get(0) == null) {
							tuples.put(0, ((IntField)t.getField(0)).getValue());
						}
						else {
							tuples.put(0,(int)tuples.get(0)+((IntField)t.getField(0)).getValue());
						}
						break;
				}
			}
		}
		else {
			if(td.getType(1) == Type.STRING) {
				switch(AggOp) {
					case AVG:
						break;
					case SUM:
						break;
					case MAX:
						if(tuples.get(t.getField(0)) == null) {
							tuples.put(t.getField(0), ((StringField)t.getField(0)).getValue());
						}
						else {
							if(((StringField)t.getField(0)).getValue().compareTo(tuples.get(t.getField(0)).toString())==1) {
			        			tuples.put(t.getField(0), ((StringField)t.getField(0)).getValue());
							}
						}
						break;
					case MIN:
						if(tuples.get(t.getField(0)) == null) {
							tuples.put(t.getField(0), ((StringField)t.getField(0)).getValue());
						}
						else {
							if(((StringField)t.getField(0)).getValue().compareTo(tuples.get(t.getField(0)).toString())==-1) {
			        			tuples.put(t.getField(0), ((StringField)t.getField(0)).getValue());
							}
						}
						break;
					case COUNT:
						if(tuples.get(t.getField(0)) == null) {
							tuples.put(t.getField(0), 1);
						}
						else {
							tuples.put(t.getField(0),(int)tuples.get(t.getField(0))+1);
						}
						break;
				}
			}
			if(td.getType(1) == Type.INT) {
				switch(AggOp) {
					case MAX:
						if(tuples.get(t.getField(0)) == null) {
							tuples.put(t.getField(0), ((IntField)t.getField(1)).getValue());
						}
						else {
							if((int)tuples.get(t.getField(0))<((IntField)t.getField(1)).getValue()) {
			        			tuples.put(t.getField(0), ((IntField)t.getField(1)).getValue());
			        		}
						}
						break;
					case MIN:
						if(tuples.get(t.getField(0)) == null) {
							tuples.put(t.getField(0), ((IntField)t.getField(1)).getValue());
						}
						else {
							if((int)tuples.get(t.getField(0))>((IntField)t.getField(1)).getValue()) {
			        			tuples.put(t.getField(0), ((IntField)t.getField(1)).getValue());
			        		}
						}
						break;
					case AVG:
						if(tuples.get(t.getField(0)) == null) {
							tuples.put(t.getField(0), ((IntField)t.getField(0)).getValue());
			        		counter.put(t.getField(0), 1);
						}
						else {
							counter.put(t.getField(0), (int)counter.get(t.getField(0))+1);
			        		tuples.put(t.getField(0), ((counter.get(t.getField(0))-1*((int)tuples.get(t.getField(0)))+(((IntField)t.getField(1)).getValue()))/counter.get(t.getField(0))));
						}
						break;
					case COUNT:
						if(tuples.get(t.getField(0)) == null) {
							tuples.put(t.getField(0), 1);
						}
						else {
							tuples.put(t.getField(0),(int)tuples.get(t.getField(0))+1);
						}
						break;
					case SUM:
						if(tuples.get(t.getField(0)) == null) {
							tuples.put(t.getField(0), ((IntField)t.getField(1)).getValue());
						}
						else {
							tuples.put(t.getField(0),(int)tuples.get(t.getField(0))+((IntField)t.getField(1)).getValue());
						}
						break;
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
		ArrayList<Tuple> Tuples = new ArrayList<Tuple>();
		if(groupBy) {
			Iterator itr = tuples.entrySet().iterator();
		    while (itr.hasNext()) {
		        Map.Entry pair = (Map.Entry)itr.next();
		        itr.remove();
				Tuple newTuple = new Tuple(td);
				if(td.getType(0)==Type.INT) {
					newTuple.setField(0, new IntField((int)pair.getValue()));
				}
				else {
					newTuple.setField(0, new StringField(pair.getValue().toString()));
				}
				if(td.getType(1)==Type.INT) {
					newTuple.setField(1, new IntField((int)pair.getValue()));
				}
				else {
					newTuple.setField(1, new StringField(pair.getValue().toString()));
				}
				Tuples.add(newTuple);
			}
		}
		else {
			for(int i=0;i<tuples.size();i++) {
				Tuple newTuple = new Tuple(td);
				if(td.getType(0)==Type.INT) {
					newTuple.setField(i, new IntField((int)tuples.get(i)));
				}
				else {
					newTuple.setField(i, new StringField(tuples.get(i).toString()));
				}
				Tuples.add(newTuple);
			}
		}
		return Tuples;
	}
}
