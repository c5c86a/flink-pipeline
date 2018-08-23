package mygroupid;

/**
 * Returns last CommonPOJO added with isLessThanMovingAvg calculated.
 * It does not support distributive aggregations as when merging,
 * if x has a later lastElementAdded, it is dropped.
 */
public class AverageAccumulator {
    long count;
    long sum;
    CommonPOJO lastElementAdded;
    public AverageAccumulator(long count, long sum){
        this.count = count;
        this.sum = sum;
        this.lastElementAdded = null;
    }
    public AverageAccumulator add(CommonPOJO in) {
        this.sum += in.deliveryDelay;
        this.count += 1L;
        this.lastElementAdded = in;
        return this;
    }
    public AverageAccumulator merge(AverageAccumulator x){
        this.count += x.count;
        this.sum += x.sum;
        return this;
    }
    public CommonPOJO getResult(){
        this.lastElementAdded.isLessThanMovingAvg = (((double) this.sum) / this.count) < 90000000;
        return this.lastElementAdded;
    }

}
