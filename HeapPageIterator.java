package simpledb;

import java.util.*;

/**
 * Iterator for HeapPage.java
 */
 
 public class HeapPageIterator implements Iterator {
	
	public int numTuples;
	public int numSlots;
	public int currentIndex;
	public HeapPage heapPage;
		
	 
	public HeapPageIterator(HeapPage heapPage_) {
		heapPage = heapPage_;
		currentIndex = -1;
		numTuples = heapPage.tuples.length;
		numSlots = heapPage.numSlots;
	}
	 


	public boolean hasNext() {
		while (currentIndex < numSlots && !heapPage.isSlotUsed(currentIndex)) {
			currentIndex += 1;
		}
		if (currentIndex < numSlots) {
			return true;
		}
		else {
			return false;
		}
	}

	public Object next() {

		if (hasNext()) {
			currentIndex++;
			return heapPage.tuples[currentIndex - 1];
		}
		else {
			throw new NoSuchElementException("HeapPageIterator - no next tuples");
		}
	}
	
	public void remove() throws UnsupportedOperationException {
        throw new UnsupportedOperationException("HeapPageIterator - Remove is not supported.");
    }
 }
