package ai.djl.ndarray;

import it.unimi.dsi.fastutil.Arrays;
import it.unimi.dsi.fastutil.Swapper;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntComparator;

/**
 * A special class that Aggregates the {@link NDArray} that are identified by a specific String key
 * <p>
 *     Implementation of this class is not Thread-safe
 * </p>
 */
public class NDArraysAggregator {

    public String[] keys;

    public NDArray batchedNDArray;

    public final transient LinkedWrapperAndComparator swapperAndComparator = new LinkedWrapperAndComparator();

    public NDArraysAggregator(){
    }

    /**
     * Add newKeys and a newBatchedNDArray which is going to sum overlapping keys
     */
    public void aggregate(String[] newKeys, NDArray newBatchedNDArray){
        if(newKeys.length == 0) throw new RuntimeException("WHY ARRAY IS EMPTY ");
        if(keys == null){
            // First time is sorted
            keys = newKeys;
            batchedNDArray = newBatchedNDArray;
        }
        else{
            batchedNDArray.resume();
            swapperAndComparator.createIndices();
            Arrays.mergeSort(0, keys.length, swapperAndComparator, swapperAndComparator);
            batchedNDArray = batchedNDArray.get(BaseNDManager.getManager().create(swapperAndComparator.indices));
            int j;
            IntArrayList toBeAddedIndices = new IntArrayList();
            for (int i = 0; i < newKeys.length; i++) {
                if((j=java.util.Arrays.binarySearch(keys, newKeys[i])) > 0){
                    // Found in this array
                    batchedNDArray.get(j).addi(newBatchedNDArray.get(i));
                }else{
                    // Not found in this array
                    toBeAddedIndices.add(i);
                }
            }
            if(!toBeAddedIndices.isEmpty()){
                int[] tobeAddedIndicesArray = toBeAddedIndices.toIntArray();
                String[] updatedKeys = new String[tobeAddedIndicesArray.length + keys.length];
                System.arraycopy(keys,0,updatedKeys,0, keys.length);
                for (int i = 0; i < tobeAddedIndicesArray.length; i++) {
                    updatedKeys[keys.length + i] = newKeys[tobeAddedIndicesArray[i]];
                }
                batchedNDArray = batchedNDArray.concat(newBatchedNDArray.get(BaseNDManager.getManager().create(tobeAddedIndicesArray)),0);
                keys = updatedKeys;
            }
        }
        batchedNDArray.delay();
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



   public class LinkedWrapperAndComparator implements Swapper, IntComparator {
        public int[] indices;

        public void createIndices(){
            indices = new int[keys.length];
            for (int i = 0; i < keys.length; i++) {
                indices[i] = i;
            }
        }

       @Override
       public void swap(int a, int b) {
            String tmpStr = keys[a];
            keys[a] = keys[b];
            keys[b] = tmpStr;
            int tmpInt = indices[a];
            indices[a] = indices[b];
            indices[b] = tmpInt;
       }

       @Override
       public int compare(int k1, int k2) {
           return keys[k1].compareTo(keys[k2]);
       }
   }


}
