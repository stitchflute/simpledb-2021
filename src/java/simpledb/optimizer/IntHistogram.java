package simpledb.optimizer;

import simpledb.execution.Predicate;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
    
    private int min;
    private int max;
    private int histogram[];
    private int range;
    private int totalNum;

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
    	// some code goes here
        this.min = min;
        this.max = max;
        this.range = ((max-min) < buckets) ? 1 : (max-min+buckets-1)/buckets;
        this.histogram = new int[buckets];
        this.totalNum = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        if(v < min || v > max) return;
        int idx = (v-min) / range;
        if(idx < 0) 
            idx = 0;
        else if (idx >= histogram.length)
            idx = histogram.length-1;
        histogram[idx]++;
        totalNum++;
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
    public double estimateSelectivity(Predicate.Op op, int v) {

    	// some code goes here
        double selectivity = 0.0;
        if (op.equals(Predicate.Op.LESS_THAN)) {
            if(v <= min) return 0.0;
            if(v >= max) return 1.0;
            int idx = (v-min)/range;
            int width = (idx == histogram.length-1) ? max - min - range*idx : range;
            int b_left = min + range*idx;
            int sum = 0;
            for(int i = 0; i < idx; ++i)
                sum += histogram[i];
            sum += histogram[idx] * (v - b_left) / width;
            selectivity = sum*1.0 / totalNum;
            return selectivity;
        }
        if(op.equals(Predicate.Op.LESS_THAN_OR_EQ))
            return estimateSelectivity(Predicate.Op.LESS_THAN, v+1);
        if(op.equals(Predicate.Op.GREATER_THAN))
            return 1 - estimateSelectivity(Predicate.Op.LESS_THAN_OR_EQ, v);
        if(op.equals(Predicate.Op.GREATER_THAN_OR_EQ))
            return 1 - estimateSelectivity(Predicate.Op.LESS_THAN, v);
        if(op.equals(Predicate.Op.EQUALS))
            return estimateSelectivity(Predicate.Op.LESS_THAN_OR_EQ, v) - 
                    estimateSelectivity(Predicate.Op.LESS_THAN, v);
        if(op.equals(Predicate.Op.NOT_EQUALS))
            return 1 - estimateSelectivity(Predicate.Op.EQUALS, v);
        System.out.println("estSelect op is not valid");
        return 0.0;    
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
        // some code goes here
        return null;
    }
}
