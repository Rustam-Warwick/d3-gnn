package ai.djl.ndarray;

import java.util.Comparator;

/**
 * A special class that Aggregates the {@link NDArray} that are identified by a specific String key
 * <p>
 *     Implementation of this class is not Thread-safe
 * </p>
 */
public class NDArraysAggregator {

    public String[] keys;

    public NDArray batchedNDArray;

    protected transient ArrayIndexComparator comparator;

    public NDArraysAggregator(){
        comparator = new ArrayIndexComparator();
    }

    /**
     * Add newKeys and a newBatchedNDArray which is going to sum overlapping keys
     */
    public void aggregate(String[] newKeys, NDArray newBatchedNDArray){
        if(keys == null){
            // First time is sorted
            keys = newKeys;
            batchedNDArray = newBatchedNDArray;
            newBatchedNDArray.delay();
        }
        else{
            System.out.println("SALAM");
        }
    }

    /**
     * Check if this is empty
     */
    public boolean isEmpty(){
        return keys == null || keys.length == 0;
    }

    /**
     * Clear and destroy the array
     */
    public void clear(){
        keys = null;
        batchedNDArray.resume();
        batchedNDArray = null;
    }



    /**
     * Helper method for getting the sorted indices
     */
    public class ArrayIndexComparator implements Comparator<Integer>
    {

        public Integer[] createIndexArray()
        {
            Integer[] indexes = new Integer[keys.length];
            for (int i = 0; i < keys.length; i++)
            {
                indexes[i] = i; // Autoboxing
            }
            return indexes;
        }

        @Override
        public int compare(Integer index1, Integer index2)
        {
            // Autounbox from Integer to int to use as array indexes
            return keys[index1].compareTo(keys[index2]);
        }
    }


}
