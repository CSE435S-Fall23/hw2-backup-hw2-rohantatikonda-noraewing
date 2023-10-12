package hw1;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * A heap file stores a collection of tuples. It is also responsible for managing pages.
 * It needs to be able to manage page creation as well as correctly manipulating pages
 * when tuples are added or deleted.
 * @author Sam Madden modified by Doug Shook
 *
 */
public class HeapFile {
	
	public static final int PAGE_SIZE = 4096;
	private TupleDesc td;
	private File f;
	/**
	 * Creates a new heap file in the given location that can accept tuples of the given type
	 * @param f location of the heap file
	 * @param types type of tuples contained in the file
	 */
	public HeapFile(File f, TupleDesc type) {
		//your code here
		this.td = type;
		this.f = f;
	
	}
	
	public File getFile() {
		return f;
	}
	
	public TupleDesc getTupleDesc() {
		return td;
	}
	
	/**
	 * Creates a HeapPage object representing the page at the given page number.
	 * Because it will be necessary to arbitrarily move around the file, a RandomAccessFile object
	 * should be used here.
	 * @param id the page number to be retrieved
	 * @return a HeapPage at the given page number
	 */
	public HeapPage readPage(int id) {
		//your code here
		RandomAccessFile reader; 
		byte[] inputStream = new byte[PAGE_SIZE];
		try {
			reader = new RandomAccessFile(f, "r");
			reader.seek(PAGE_SIZE * id);
			reader.read(inputStream);
			reader.close();
			return new HeapPage(id, inputStream, this.getId());
		}
		catch (Exception e) {
			System.out.println("exception in reading in page");
		}
		return null;
	}
	
	/**
	 * Returns a unique id number for this heap file. Consider using
	 * the hash of the File itself.
	 * @return
	 */
	public int getId() {
		return f.hashCode();
	}
	
	/**
	 * Writes the given HeapPage to disk. Because of the need to seek through the file,
	 * a RandomAccessFile object should be used in this method.
	 * @param p the page to write to disk
	 * @throws IOException 
	 */
	public void writePage(HeapPage p) throws IOException {
		//your code here
		RandomAccessFile reader;
		reader = new RandomAccessFile(f, "rw");
		reader.seek(PAGE_SIZE * p.getId());
		reader.write(p.getPageData());
		reader.close();
	}
	
	/**
	 * Adds a tuple. This method must first find a page with an open slot, creating a new page
	 * if all others are full. It then passes the tuple to this page to be stored. It then writes
	 * the page to disk (see writePage)
	 * @param t The tuple to be stored
	 * @return The HeapPage that contains the tuple
	 * @throws Exception 
	 */
	public HeapPage addTuple(Tuple t) throws Exception {
		//your code here
		//iterates through all pages and all slots to find empty slot
		if (t.getDesc().equals(td) == false) {
			throw new Exception("Given tuple does not have same structure as existing tuple/tuples");
		}
		for(int i = 0; i < getNumPages(); ++i) {
			HeapPage currPage = readPage(i);
			for(int j = 0; j < currPage.getNumSlots(); ++j) {
				if( currPage.slotOccupied(j) == false) {
					currPage.addTuple(t);
					RandomAccessFile reader;
					try {
						byte[] inputStream = currPage.getPageData();
						reader = new RandomAccessFile(f, "rw");
						reader.seek(PAGE_SIZE * i);
						reader.write(inputStream);
						reader.close();
					}
					catch (Exception e) {
						System.out.println("exception in writing to disk");
					}
					return currPage;
				}
			}
		}
		//creates new page if no empty slots are found
		byte[] newData = new byte[PAGE_SIZE];
		int newID = getId();
		HeapPage newPage = new HeapPage(getNumPages(), newData, newID);
		newPage.addTuple(t);
		writePage(newPage);
		return newPage;
	}
	
	/**
	 * This method will examine the tuple to find out where it is stored, then delete it
	 * from the proper HeapPage. It then writes the modified page to disk.
	 * @param t the Tuple to be deleted
	 * @throws Exception 
	 */
	public void deleteTuple(Tuple t) throws Exception{
		//your code here
		if (t.getDesc().equals(td) == false) {
			throw new Exception("Given tuple does not have same structure as existing tuple/tuples");
		}
		int pId = t.getPid();
		HeapPage currPage = readPage(pId);
		currPage.deleteTuple(t);
		writePage(currPage);
	}
	
	/**
	 * Returns an ArrayList containing all of the tuples in this HeapFile. It must
	 * access each HeapPage to do this (see iterator() in HeapPage)
	 * @return
	 */
	public ArrayList<Tuple> getAllTuples() {
		//your code here
		ArrayList<Tuple> allTuples = new ArrayList<Tuple>();
		for(int i = 0; i < getNumPages(); ++i) {
			HeapPage currPage = readPage(i);
			Iterator<Tuple> allTuplesItr = currPage.iterator();
			while(allTuplesItr.hasNext() == true) {
				allTuples.add(allTuplesItr.next());
			}
		}
		return allTuples;
	}
	
	/**
	 * Computes and returns the total number of pages contained in this HeapFile
	 * @return the number of pages
	 */
	public int getNumPages() {
		//your code here
		if(f.length() % PAGE_SIZE == 0) {
			return (int) (f.length()/PAGE_SIZE);
		}
		return (int) (f.length()/PAGE_SIZE)+1;
	}
}