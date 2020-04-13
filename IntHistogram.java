package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
	
	int numBuckets; 
	int minValue; 
	int maxValue;
	int[] heightsHist; // Where index is the bucket number, given from 0 to numBuckets-1.
	int[] leftBound;
	int[] rightBound;
	int bucketSize; // If bucket is 1-10, bucket size = 10
	int numTuples; // This is the sum of all the heights. 

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	
    	minValue = min;
    	maxValue = max;
    	
    	// Since they pass in dirty bucket sizes, clean this to ensure this is the correct buck size we need.
    	int maxPossibleBuckets = max - min + 1; //ie, 10-1+1.
    	if (buckets > maxPossibleBuckets) {
    		numBuckets = maxPossibleBuckets;
    	}
    	else {
    		numBuckets = buckets;
    	}
    	
    	heightsHist = new int[numBuckets]; // The heights can only be filled at the addValue function. //Goes from 0 to num-1
    	leftBound = new int[numBuckets];   // However the left and right bounds are computed here.
    	rightBound = new int[numBuckets];
    	
    	// Notice since we only have ints not doubles, the bounds look like: 1-10, 11-20, 21-30 ... 91-100.
    	// Getting left bounds means inputting min once and then adding 10 ("box size") continuously.
    	// Getting right bound means adding 10 - 1 to get the first right bound. And then adding 10 continuously.
    	int bucketSize = (maxValue - minValue + 1) / numBuckets;
    	this.bucketSize = bucketSize;
    	
    	leftBound[0] = minValue;
    	for (int i=1; i<leftBound.length; i++) {
    		leftBound[i] = leftBound[i-1] + bucketSize;
    	}
    	rightBound[0] = leftBound[0] + bucketSize - 1;
    	for (int i=1; i<rightBound.length; i++) {
    		rightBound[i] = rightBound[i-1] + bucketSize;
    	}
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// The value v has to be >= and <= the leftBound and rightBound of a particular box i. 
    	for (int i=0; i<heightsHist.length; i++) {
    		
    		if ((v >= leftBound[i]) && (v <= rightBound[i])) {
    			heightsHist[i]++;
    			
    			// Each time you add a value to Histogram, you can update numTuples. 
    	    	numTuples++;
    		}
    	}
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) { //Predicate is like = , operand is like 5. Check the histogram.
    	// First, get the bucket that has our value v, and  store it in "bucket". May or may not be found.
    	int bucket = 0;
    	int found = 0;
    	for (int i=0; i<numBuckets; i++) {
    		if ((v >= leftBound[i])&& (v <= rightBound[i])) {
    			bucket = i;
    			found = 1;
    		}
    	}
    	
    	
    	// Next, get height of bucket. (initialize, this may not even be valid if value = -1 and this is lower than our min.)
    	int height = heightsHist[bucket];
    	double selectivity = 0;
    	
    	if (op == op.EQUALS) {
    		if (v < minValue) {
				return 0;
			}
			else if (v > maxValue) {
				return 0;
			}
    		selectivity = (((double) height / (double) bucketSize) / (double) numTuples); //works for equals and != because we dont do the right-left thing.
    	}
    	else if (op == op.NOT_EQUALS) {
    		if (v < minValue) {
				return 1;
			}
			else if (v > maxValue) {
				return 1;
			}
    		selectivity = 1 - (((double) height / (double) bucketSize) / (double) numTuples);;
    	}
    	
    	
    	else if (op == op.GREATER_THAN) {
    		double rangePart; //Now, we have to find the range part.
    		double fractionOfThisBucket;
    		
    		if (found == 1) {
    			fractionOfThisBucket = ((rightBound[bucket] - v ) / bucketSize) * (height / numTuples);
    		}
    		else {
    			fractionOfThisBucket = 0; //If didn't find, like val = -1, and our min = 1, there is no fraction of this buck.
    			
    			//If dont find, either, value less than min, or greater than than max, or somewhere in middle.
    			if (v < minValue) {
    				return 1;
    			}
    			else if (v > maxValue) {
    				return 0;
    			}
    			else {
    				//Need to find the closest bucket for which we would need to start checking at. 
    				//Looking at tests we dont need to implement this though.
    			}
    		}

    		
    		double remainingBuckets = 0;
    		if (bucket < numBuckets-1) { //Ensure that the index of bucket+1 is valid for this array. numBuck - 1 is last inex so must be less than this.
    			for (int i = bucket+1; i < numBuckets; i++) {
	    			//These buckets now contibute all of their selectivity.
    				//Now have new heights each time.
    				
    				height = heightsHist[i];
    				remainingBuckets += ((rightBound[i] - leftBound[i] + 1) / bucketSize) * (height );
    				//((rightBound[i] - leftBound[i]) / bucketSize) * (height / numTuples); //This might be a problem because we do right-left, so if they are same, this is 0. and if 1-2 this is 1.. should be 2? ?? --> YES !
	    		}
    		}
	    		
    		rangePart = (fractionOfThisBucket + remainingBuckets)/numTuples;
    		selectivity = rangePart;
    		
    	}
    	else if (op == op.GREATER_THAN_OR_EQ) {
    		double rangePart; //Now, we have to find the range part.
    		double fractionOfThisBucket = 0;
    		double equalityPart = 0;
    		
    		if (found == 1) {
    			//Now, we have to both take a fraction as well as a slice (for the part of equality).
    			fractionOfThisBucket = ((rightBound[bucket] - v ) / bucketSize) * (height / numTuples);
    			equalityPart = (((double) height / (double) bucketSize) / (double) numTuples);
    			
    		}
    		else {
    			//fractionOfThisBucket = 0; //If didn't find, like val = -1, and our min = 1, there is no fraction of this buck.
    			
    			//If dont find, either, value less than min, or greater than than max, or somewhere in middle.
    			if (v < minValue) {
    				return 1;
    			}
    			else if (v > maxValue) {
    				return 0;
    			}
    			else {
    				//Need to find the closest bucket for which we would need to start checking at. 
    				//Looking at tests we dont need to implement this though.
    			}
    		}
    		
    		
    		
    		double remainingBuckets = 0;
    		if (bucket < numBuckets-1) { //Ensure that the index of bucket+1 is valid for this array. numBuck - 1 is last inex so must be less than this.
    			for (int i = bucket+1; i < numBuckets; i++) {
	    			//These buckets now contibute all of their selectivity.
    				//Now have new heights each time.
    				height = heightsHist[i];
    				remainingBuckets += ((rightBound[i] - leftBound[i] + 1) / bucketSize) * (height );
	    		}
    		}
	    		
    		rangePart = (fractionOfThisBucket + remainingBuckets)/numTuples;
    		selectivity = rangePart + equalityPart;
    	}
    	else if (op == op.LESS_THAN) {
    		
    		double rangePart; //Now, we have to find the range part.
    		double fractionOfThisBucket = 0;
    		
    		if (found == 1) {
    			fractionOfThisBucket = ((v - leftBound[bucket]) / bucketSize) * (height / numTuples);
    			
    		}
    		else {
    			fractionOfThisBucket = 0; //If didn't find, like val = -1, and our min = 1, there is no fraction of this buck.
    			
    			//If dont find, either, value less than min, or greater than than max, or somewhere in middle.
    			if (v < minValue) {
    				return 0;
    			}
    			else if (v > maxValue) {
    				return 1;
    			}
    			else {
    				//Need to find the closest bucket for which we would need to start checking at. 
    				//Looking at tests we dont need to implement this though.
    			}
    		}

    		
    		double remainingBuckets = 0;
    		if (bucket > 0) { //Now, have to go downwards instead. 
    			for (int i = bucket - 1; i >= 0; i--) {
	    			//These buckets now contibute all of their selectivity.
    				//Now have new heights each time.
    				
    				//System.out.println("bucketnum = " + i);
    				height = heightsHist[i];
    				//System.out.println("height" + height);
    				remainingBuckets += ((rightBound[i] - leftBound[i] + 1) / bucketSize) * (height );
    				//((rightBound[i] - leftBound[i]) / bucketSize) * (height / numTuples); //This might be a problem because we do right-left, so if they are same, this is 0. and if 1-2 this is 1.. should be 2? ?? --> YES !
    				//System.out.println("nums foundi n buckets increase: " + remainingBuckets);
	    		}
    		}
	    		
    		rangePart = (fractionOfThisBucket + remainingBuckets)/numTuples;
    		selectivity = rangePart;

    	}
    	else if (op == op.LESS_THAN_OR_EQ) {
    		double rangePart; //Now, we have to find the range part.
    		double fractionOfThisBucket = 0;
    		double equalityPart = 0;
    		
    		if (found == 1) {
    			fractionOfThisBucket = ((v - leftBound[bucket]) / bucketSize) * (height / numTuples);
    			equalityPart = (((double) height / (double) bucketSize) / (double) numTuples);
    		}
    		else {
    			fractionOfThisBucket = 0; //If didn't find, like val = -1, and our min = 1, there is no fraction of this buck.
    			
    			//If dont find, either, value less than min, or greater than than max, or somewhere in middle.
    			if (v < minValue) {
    				return 0;
    			}
    			else if (v > maxValue) {
    				return 1;
    			}
    			else {
    				//Need to find the closest bucket for which we would need to start checking at. 
    				//Looking at tests we dont need to implement this though.
    			}
    		}

    		double remainingBuckets = 0;
    		if (bucket > 0) { //Now, have to go downwards instead. 
    			for (int i = bucket - 1; i >= 0; i--) {
	    			//These buckets now contibute all of their selectivity.
    				//Now have new heights each time.
    				
    				//System.out.println("bucketnum = " + i);
    				height = heightsHist[i];
    				//System.out.println("height" + height);
    				remainingBuckets += ((rightBound[i] - leftBound[i] + 1) / bucketSize) * (height );
    				//((rightBound[i] - leftBound[i]) / bucketSize) * (height / numTuples); //This might be a problem because we do right-left, so if they are same, this is 0. and if 1-2 this is 1.. should be 2? ?? --> YES !
    				//System.out.println("nums foundi n buckets increase: " + remainingBuckets);
	    		}
    		}
    		rangePart = (fractionOfThisBucket + remainingBuckets)/numTuples;
    		selectivity = rangePart + equalityPart;
    	}
    	else if (op == op.LIKE) {
    		System.out.println("Operator is like. what to do?");
    	}
    	else {
    		System.out.println("Impossible to reach this operator");
    	}
    	

        return selectivity;
    }
    
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
    	System.out.print("\n");
        for(int i=0; i< numBuckets; i++) {
        	System.out.print("   " + heightsHist[i] + "   ");
        	
        }
        System.out.print("\n");
        for(int i=0; i< numBuckets; i++) {
        	System.out.print(leftBound[i] + "-" + rightBound[i] + " ");
        	
        }
        return null;
    }
}

