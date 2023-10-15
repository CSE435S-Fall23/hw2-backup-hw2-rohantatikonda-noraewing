package hw1;

import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

/**
 * This class represents a tuple that will contain a single row's worth of information
 * from a table. It also includes information about where it is stored
 * @author Sam Madden modified by Doug Shook
 *
 */
public class Tuple {
	private TupleDesc tupleDesc;
	private Map<Integer, Field> fields = new HashMap<Integer, Field>();
	private int Pid;
	private int Id;
	
	/**
	 * Creates a new tuple with the given description
	 * @param t the schema for this tuple
	 */
	public Tuple(TupleDesc t) {
		this.tupleDesc = t;
//		this.fields = new Field[tupleDesc.numFields()];
		this.Pid = 0;
		this.Id = -1;
	}
	
	public TupleDesc getDesc() {
		return tupleDesc;
	}
	
	/**
	 * retrieves the page id where this tuple is stored
	 * @return the page id of this tuple
	 */
	public int getPid() {
		return Pid;
	}

	public void setPid(int pid) {
		this.Pid = pid;
	}

	/**
	 * retrieves the tuple (slot) id of this tuple
	 * @return the slot where this tuple is stored
	 */
	public int getId() {
		return Id;
	}

	public void setId(int id) {
		this.Id = id;
	}
	
	public void setDesc(TupleDesc td) {
		this.tupleDesc = td;
	}
	
	/**
	 * Stores the given data at the i-th field
	 * @param i the field number to store the data
	 * @param v the data
	 */
	public void setField(int i, Field v) {
		this.fields.put(i,v);
	}
	
	public Field getField(int i) {
		return fields.get(i);
	}
	
	/**
	 * Creates a string representation of this tuple that displays its contents.
	 * You should convert the binary data into a readable format (i.e. display the ints in base-10 and convert
	 * the String columns to readable text).
	 */
	public String toString() {
		StringBuilder str = new StringBuilder();
		for(int i = 0; i < fields.size(); i++) {
			str.append(getField(i).toString());
		}
		return str.toString();
	}
}

	