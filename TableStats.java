package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


//Why is this wrong ?? Or is it num attributes. 


/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {
	DbFile file;
	TupleDesc tupleDescOfFile;
	int numAttributes;
	ArrayList<Object> histogramArray; //Contains IntHistogram and StringHistogram objects.
	int tableID;
	int ioCostPerPage;
	int numTuples;
	int numPages;
	

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;
    
    int tableId;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
    	
    	// 0: Get file, tupledesc, and other basic values.
    	DbFile file = Database.getCatalog().getDatabaseFile(tableid);
    	TupleDesc tupleDescOfFile = Database.getCatalog().getTupleDesc(tableid);
    	int numAttributes = tupleDescOfFile.getSize(); //How many histograms we will store in our Histogram Arraylist, for indexing.
    	ArrayList<Object> histogramArray = new ArrayList<Object>(); //this will contain histograms of both String type and Int type.
    	tableID = tableid;
    	this.ioCostPerPage = ioCostPerPage;
    	this.file = file;
    	this.tupleDescOfFile = tupleDescOfFile; 
    	//this.numAttributes = numAttributes;  
    	this.histogramArray = histogramArray;
    	
    	
    	// 0: Then, to create the histogram array (histogram for each attribute), note that we need to know, min, max for each attribute. 
    	// Number of histogram buckets is given to us (NUM_HIST_BINS), above.
    	// So, will need to iterate once through all  tuples first, to see what the min max values are, before we can create initialize histogram (=new hist())
    	// Only after that can we start adding their actual values to the histogram.
    	// Note: If String, we don't need to get a min/max, we only need numBuckets. So if we see a string attribute, set arbitrary 0. 
    	
    	HeapFile castAsHeapFile = (HeapFile) file;
    	
    	int theNumPages = castAsHeapFile.numPages();
    	numPages = theNumPages;
    	
    	int pageNum;
    	HeapPage page;
    	
    	// 1: First, set initial min/max. --> Are on first page num and are looking at tuple 1.
    	pageNum = 0; 
    	HeapPageId heapPageID = new HeapPageId(tableID, pageNum);
    	page = (HeapPage) (file.readPage(heapPageID));
    	//page = bPool.getPage(tid, heapPageID, Permissions.READ_ONLY); I thought we needed this to get pages, but we dont have tid. Not a problem? 
    		
    	// Then, get first Tuple 
    	Iterator<Tuple> iterTuple = page.iterator();
    	Tuple tuple = iterTuple.next();
    		
    	// Set initial min/max 
    	ArrayList<Field> listOfFields = tuple.al;
    	numAttributes = listOfFields.size();
    	int minValues[] = new int[numAttributes];
    	int maxValues[] = new int[numAttributes];
    	//System.out.println(numAttributes);
    	for (int i=0; i<listOfFields.size(); i++) {
    		if (listOfFields.get(i).getType() == Type.INT_TYPE) {
    			IntField intField = (IntField) listOfFields.get(i);
    			Integer value = intField.getValue();
    			minValues[i] = value;
    			maxValues[i] = value;
    		}
    		else if (listOfFields.get(i).getType() == Type.STRING_TYPE) {
    			minValues[i] = 0;
    			maxValues[i] = 0;
    		} 
    		else {
    			System.out.println("This is some weird type. ???? ");
    		}
    	}
    	
    	// 2: Then go thru all pages/ tuples to get the real min max for each attribute.
    	int numberOfTuples = 0;
    	for (pageNum = 0; pageNum < numPages; pageNum++) { //For each  page; ...
    		heapPageID = new HeapPageId(tableID, pageNum);
    		page = (HeapPage) (file.readPage(heapPageID));
    		
    		// Then, get a Tuple Iterator for the page. 
    		iterTuple = page.iterator();
    		while (iterTuple.hasNext()) { // For each tuple; ... 
    			tuple = iterTuple.next();
    			numberOfTuples++; 
    			listOfFields = tuple.al;
    			
    			for (int i=0; i<numAttributes; i++) { // For all attributes; ...
        			if (listOfFields.get(i).getType() == Type.INT_TYPE) {
        				IntField intField = (IntField) listOfFields.get(i);
        				int value = intField.getValue();
        				
        				if (value < minValues[i]) {
        					minValues[i] = value;
        				}
        				if (value > maxValues[i]) {
        					maxValues[i] = value;
        				}
        			}
        			else if (listOfFields.get(i).getType() == Type.STRING_TYPE) {
        				minValues[i] = 0;
        				maxValues[i] = 0;
        			} 
        			else {
        				System.out.println("This is some weird type. ???? ");
        			}
        		}
    		}
    	}
    	numTuples = numberOfTuples;

    	
    	// 3: Now that we know min/max, we can actually create the histograms. 
    	
    	for (int i=0; i<numAttributes; i++) {
    		Type type = tupleDescOfFile.getFieldType(i); 
    		if (type == Type.INT_TYPE) { 
    			IntHistogram newIntHistogram = new IntHistogram(NUM_HIST_BINS, minValues[i], maxValues[i]);
    			this.histogramArray.add(newIntHistogram);
    		}
    		else if (type == Type.STRING_TYPE) { 
    			StringHistogram newStringHistogram = new StringHistogram(NUM_HIST_BINS);
    			histogramArray.add(newStringHistogram);
    		}
    		else {
    			System.out.println("Dont know this type!");
    		}
    		
    	}
    	
    	// 4: Now that histograms created, can add values. Do same thing as above ~thru all pages/tuples~ but actually add the values. 
    	for (pageNum = 0; pageNum < numPages; pageNum++) { //For each  page; ...
    		heapPageID = new HeapPageId(tableID, pageNum);
    		page = (HeapPage) (file.readPage(heapPageID));
    		
    		// Then, get a Tuple Iterator for the page. 
    		iterTuple = page.iterator();
    		while (iterTuple.hasNext()) { // For each tuple; ... 
    			tuple = iterTuple.next();
    			listOfFields = tuple.al;
    			
    			for (int i=0; i<numAttributes; i++) { // For all attributes; ...
        			if (listOfFields.get(i).getType() == Type.INT_TYPE) {
        				IntField intField = (IntField) listOfFields.get(i);
        				Integer value = intField.getValue();
        				
        				// Now, do the work to add to the histogram: 
        				IntHistogram hist = (IntHistogram) histogramArray.get(i); //This gives us our required histogram. (i as one for each attribute.)
        				hist.addValue(value);
        			}
        			else if (listOfFields.get(i).getType() == Type.STRING_TYPE) {
        				StringField stringField = (StringField) listOfFields.get(i);
        				String value = stringField.getValue();
        				
        				// Now, do the work to add to the histogram: 
        				StringHistogram hist = (StringHistogram) histogramArray.get(i); //This gives us our required histogram. (i as one for each attribute.)
        				hist.addValue(value);
        				
        			} 
        			else {
        				System.out.println("This is some weird type. ???? ");
        			}
        		}
    		}
    	}  

    	
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // Note the equation: scancost(t1) = (the number of pages in t1) x SCALING_FACTOR
    	// Im assuming scaling factor = IOCOSTPERPAGE ? 
    	
        return ((double)  ioCostPerPage  * (double) numPages);
        //IOCOSTPERPAGE
        
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) { 
    	//Selectivity is a percentage.. and we know numTuples. So, just numTuples*selectivity?
    	double result = (double) selectivityFactor * (double) numTuples;
        return (int) result; //Would be impossible for this to not be a clean int.. 
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {  // Currently not needed, this is EC !!!
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) { // age (Field), = (Predictate) , 5 (int field). 
    	
    	// We have Field, so get the histogram for it. 
    	//String nameOfField = constant.toString(); // whether its a string or integer tupe, its name will always be string type!
    	//tupleDescOfFile.fieldNameToIndex(nameOfField);
    	int indexOfHistogram = field;		
    	Type type = tupleDescOfFile.getFieldType(indexOfHistogram); 
    	
    	double selectivity = 0;
		if (type == Type.INT_TYPE) { 
			IntHistogram intHist = (IntHistogram) histogramArray.get(indexOfHistogram);
			
			IntField intFieldConstant = (IntField) constant;
			Integer integerConstant = intFieldConstant.getValue();
			
			selectivity = intHist.estimateSelectivity(op, integerConstant); 
		}
		else if (type == Type.STRING_TYPE) { 
			StringHistogram stringHist = (StringHistogram) histogramArray.get(indexOfHistogram);
			
			StringField stringFieldConstant = (StringField) constant;
			String stringConstant = stringFieldConstant.getValue();
			
			selectivity = stringHist.estimateSelectivity(op, stringConstant); 
		}
    	
        return selectivity;
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        return numTuples;
    }

}

