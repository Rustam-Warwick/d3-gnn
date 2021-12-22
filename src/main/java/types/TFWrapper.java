package types;

import org.tensorflow.Tensor;
import org.tensorflow.types.TFloat32;
import org.tensorflow.types.family.TType;

import java.util.Arrays;

public class TFWrapper<I extends TType> {
    public I value;
    public TFWrapper(){
        this.value = null;

    }
    public TFWrapper(I e){
        this.value = e;
    }

    public I getValue() {
        return value;
    }

    public void setValue(I value) {
        this.value = value;
    }

    public static void printTensor(TFloat32 tensor){
        long[]shape = tensor.shape().asArray();
        long[]tmpS = Arrays.stream(shape).map(item->item-1).toArray();
        int dims = shape.length;
        long[] vars = new long[dims];
        while(!Arrays.equals(tmpS,vars)){
            System.out.format("%f\t",tensor.getFloat(vars));
            for(int j=dims-1;j>=0;j--){
                if(j==dims-1)vars[j]++;
                if(vars[j]>=shape[j]){
                    vars[j] = 0;
                    if(j>0)vars[j-1]++;
                    System.out.printf("\n");
                }
            }
        }
        System.out.format("%f\n\n",tensor.getFloat(vars));
    }
}
