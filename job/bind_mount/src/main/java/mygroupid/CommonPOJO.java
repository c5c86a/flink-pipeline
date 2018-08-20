package mygroupid;

/**
 * A POJO with public fields, an empty constructor and toString for Flink
 * Each field is initialized to avoid NPE
 */
public class CommonPOJO {
    public String input = "";
    public Integer deliveryDelay = -1;
    public Boolean isLessThanThreshold = false;
    public Boolean isLessThanMovingAvg = false;

    public CommonPOJO() {
    }
    @Override
    public String toString() {
        return "(" +
                input + "," +
                deliveryDelay + "," +
                (isLessThanThreshold?"true":"false") + "," +
                (isLessThanMovingAvg?"true":"false") +
                ")";
    }
}

