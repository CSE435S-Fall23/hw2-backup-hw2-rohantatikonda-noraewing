package hw1;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc {

	private Type[] types;
	private String[] fields;
	
    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr array specifying the number of and types of fields in
     *        this TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
    	this.types = typeAr;
    	this.fields = fieldAr;
    }
 
    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
    	return types.length;
    }
    public String[] getFields() {
    	return fields;
    }
    public void setFields(String[] fields) {
    	this.fields = fields;
    }
    public void setTypes(Type[] type) {
    	this.types = type;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
    	if(i < 0 || i > fields.length) {
    		throw new NoSuchElementException();
    	} else {
    		return fields[i];
    	}
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int nameToId(String name) throws NoSuchElementException {
        for(int i=0; i<types.length; i++) {
        	if(getFieldName(i) == name) {
        		return i;
        	}
        	else {
        	}
        }
    	throw new NoSuchElementException();
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getType(int i) throws NoSuchElementException {
    	if(i < 0 || i > types.length) {
    		throw new NoSuchElementException();
    	} else {
    		return types[i];
    	}
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
    	int size = 0;
    	for(Type type: types) {
    		//Type type = types[i];
    		if(Type.INT == type) {
    			size = size + 4;
    		} else {
    			size = size + 129;
    		}
    	}
    	return size;
    }
    
    
    /**
     * Compares the specified object with this TupleDesc for equality.
     * Two TupleDescs are considered equal if they are the same size and if the
     * n-th type in this TupleDesc is equal to the n-th type in td.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
    	TupleDesc object = ((TupleDesc)o);
    	
    	if(this.getSize() != (object.getSize())) {
    		return false;
    	}
    	for(int i = 0; i < this.numFields(); i++) {
    		if(!this.getType(i).equals(object.getType(i))) {
    			return false;
    		}
    	}
    	return true;
    }
    

    public int hashCode() {
	return 0;
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * @return String describing this descriptor.
     */
    public String toString() {
    	String output = "";
    	for(int i = 0; i < numFields(); i++) {
    		output = output + getType(i) + getFieldName(i) + ", ";
    	}
    	return output;
    }
}



