package hw1;

import java.util.ArrayList;

/**
 * This class provides methods to perform relational algebra operations. It will be used
 * to implement SQL queries.
 * @author Doug Shook
 *
 */
public class Relation {

	private ArrayList<Tuple> tuples;
	private TupleDesc td;
	
	public Relation(ArrayList<Tuple> l, TupleDesc td) {
		//your code here
		this.td = td;
		this.tuples = l;
	}
	
	/**
	 * This method performs a select operation on a relation
	 * @param field number (refer to TupleDesc) of the field to be compared, left side of comparison
	 * @param op the comparison operator
	 * @param operand a constant to be compared against the given column
	 * @return
	 */
	public Relation select(int field, RelationalOperator op, Field operand) {
		//your code here
		if(tuples.isEmpty()) {
			Relation empty = new Relation(new ArrayList<Tuple>(), this.getDesc());
			return empty;
		}
		Relation currRelation = new Relation(new ArrayList<Tuple>(), this.getDesc());
		for(int i = 0; i < tuples.size(); ++i) {
			if(tuples.get(i).getField(field).compare(op, operand)) {
				currRelation.tuples.add(tuples.get(i));
			}
		}
		return currRelation;
	}
	
	/**
	 * This method performs a rename operation on a relation
	 * @param fields the field numbers (refer to TupleDesc) of the fields to be renamed
	 * @param names a list of new names. The order of these names is the same as the order of field numbers in the field list
	 * @return
	 */
	public Relation rename(ArrayList<Integer> fields, ArrayList<String> names) {
		//your code here
		Relation rename = this;
		String [] newFields = this.td.getFields();
		for(int i = 0; i < fields.size(); ++i) {
			newFields[fields.get(i)] = names.get(i);
		}
		rename.td.setFields(newFields);
		return rename;
	}
	
	/**
	 * This method performs a project operation on a relation
	 * @param fields a list of field numbers (refer to TupleDesc) that should be in the result
	 * @return
	 */
	public Relation project(ArrayList<Integer> fields) {
		//your code here
		//copying types and fields of corresponding field numbers
		Type newTypes[] = new Type[fields.size()];
		String newFields[] = new String[fields.size()];
		for(int i = 0; i < fields.size(); ++i) {
			newTypes[i] = td.getType(fields.get(i));
			newFields[i] = td.getFieldName(fields.get(i));
		}
		TupleDesc newTd = new TupleDesc(newTypes, newFields);
		this.td.setFields(newFields);
		this.td.setTypes(newTypes);
		Relation newRelation = new Relation(new ArrayList<Tuple>(), newTd);
		//creating tuples for new Relation from existing relation based on newTd and corresponding field numbers
		for(int i = 0; i < fields.size(); ++i) {
			for(int j = 0; j < tuples.size(); ++j) {
				Tuple newTuple = new Tuple(newTd);
				newTuple.setField(i, tuples.get(j).getField(fields.get(i)));
				newRelation.tuples.add(newTuple);
			}
		}
		return newRelation;
	}
	
	/**
	 * This method performs a join between this relation and a second relation.
	 * The resulting relation will contain all of the columns from both of the given relations,
	 * joined using the equality operator (=)
	 * @param other the relation to be joined
	 * @param field1 the field number (refer to TupleDesc) from this relation to be used in the join condition
	 * @param field2 the field number (refer to TupleDesc) from other to be used in the join condition
	 * @return
	 */
	public Relation join(Relation other, int field1, int field2) {
		//your code here
		Type[] newTypes = new Type[td.numFields()+other.getDesc().numFields()];
		String[] newFields = new String[td.numFields() + other.getDesc().numFields()];
		for(int i = 0; i < newTypes.length; ++i) {
			if(i < td.numFields()) {
				newTypes[i] = td.getType(i);
				newFields[i] = td.getFieldName(i);
			}
			else {
				newTypes[i] = other.getDesc().getType(i-td.numFields());
				newFields[i] = other.getDesc().getFieldName(i-td.numFields());
			}
		}
		TupleDesc newTd = new TupleDesc(newTypes, newFields);
		td.setFields(newFields);
		td.setTypes(newTypes);
		Relation newRelation = new Relation(new ArrayList<Tuple>(), newTd);
		//go through all tuples and add all those who's fields match
		for(int i = 0; i < tuples.size(); ++i) {
			for(int j = 0; j < other.getTuples().size(); ++j) {
				if(tuples.get(i).getField(field1).compare(RelationalOperator.EQ, other.getTuples().get(j).getField(field2))) {
					Tuple newTuple = new Tuple(newTd);
					for(int k = 0; k < newFields.length; ++k) {
						newTuple.setField(k, tuples.get(i).getField(k));
						newTuple.setField(k+td.numFields(), other.getTuples().get(j).getField(k));
					}
					newRelation.tuples.add(newTuple);
				}
			}
		}
		return newRelation;
	}
	
	/**
	 * Performs an aggregation operation on a relation. See the lab write up for details.
	 * @param op the aggregation operation to be performed
	 * @param groupBy whether or not a grouping should be performed
	 * @return
	 */
	public Relation aggregate(AggregateOperator op, boolean groupBy) {
		//your code here
		TupleDesc newTd = td;
		if(!groupBy) {
			if(op.equals(AggregateOperator.COUNT)) {
				Type[] newType = {Type.INT};
				newTd.setTypes(newType);
			}
		}
		else {
			if(op.equals(AggregateOperator.COUNT)) {
				Type[] newType = {newTd.getType(0) ,Type.INT};
				newTd.setTypes(newType);
			}
		}
		Aggregator newAgg = new Aggregator(op, groupBy, newTd);
		for(int i = 0; i < tuples.size(); ++i) {
			newAgg.merge(tuples.get(i));
		}
		Relation newRelation = new Relation(newAgg.getResults(), td);
		return newRelation;
	}
	
	public TupleDesc getDesc() {
		//your code here
		return this.td;
	}
	
	public ArrayList<Tuple> getTuples() {
		//your code here
		return this.tuples;
	}
	
	/**
	 * Returns a string representation of this relation. The string representation should
	 * first contain the TupleDesc, followed by each of the tuples in this relation
	 */
	public String toString() {
		//your code here
		String toString = "";
		for(int i = 0; i < td.numFields(); ++i) {
			toString.concat(td.getFields()[i]+ " Type: " + td.getType(i));
		}
		toString.concat("\n");
		for(int i = 0; i < tuples.size(); ++i) {
			for(int j = 0; j < td.numFields(); ++j) {
				toString.concat(tuples.get(i).getField(j).toString());
			}
			toString.concat("\n");
		}
		return toString;
	}
}