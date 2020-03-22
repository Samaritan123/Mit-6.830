package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int buckets;
    private int min;
    private int max;
    private int num[];
    private int width[];
    private int l[];
    private int r[];
    private int count;

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
        this.buckets = buckets;
        this.min = min;
        this.max = max;
        num = new int[buckets];
        width = new int[buckets];
        l = new int[buckets];
        r = new int[buckets];
        int k = min;
        for (int i = 0; i < buckets; i ++) {
            width[i] = (max - min + 1) / buckets;
            if (i < (max - min  + 1) % buckets) width[i] ++;
            l[i] = k; r[i] = k + width[i] - 1;
            k += width[i];
        }
    }

    private int belong(int v) {
        if (v < min || v > max) return -1;
        for (int i = 0; i < buckets; i ++) {
            if (v >= l[i] && v <= r[i]) return i;
        }
        return -1;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        int k = belong(v);
        if (k < 0) return;
        num[k] ++;
        count ++;
    }

    private float equalNum(int v) {
        int k = belong(v);
        if (k < 0) return 0;
        return (float)num[k] / width[k];
    }

    private float lessNum(int v) {
        if (v <= min) return 0;
        if (v > max) return count;
        int k = belong(v);
        float w = 0;
        for (int i = 0; i < k; i ++) {
            w += num[i];
        }
        if (v > l[k]) w += (float)num[k] / (v - l[k]);
        return w;
    }

    private float greaterNum(int v) {
        if (v >= max) return 0;
        if (v < min) return count;
        int k = belong(v);
        float w = 0;
        for (int i = k + 1; i < buckets; i ++) {
            w += num[i];
        }
        if (r[k] > v) w += (float)num[k] / (r[k] - v);
        return w;
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
        float estimateNum = 0;
        if (op == Predicate.Op.EQUALS) {
            estimateNum = equalNum(v);
        } else if (op == Predicate.Op.LESS_THAN) {
            estimateNum = lessNum(v);
        } else if (op == Predicate.Op.LESS_THAN_OR_EQ) {
            estimateNum = lessNum(v);
            estimateNum += equalNum(v);
        } else if (op == Predicate.Op.GREATER_THAN) {
            estimateNum = greaterNum(v);
        } else if (op == Predicate.Op.GREATER_THAN_OR_EQ) {
            estimateNum = greaterNum(v);
            estimateNum += equalNum(v);
        } else if (op == Predicate.Op.NOT_EQUALS) {
            estimateNum += count - equalNum(v);
        }
        return estimateNum / count;
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
