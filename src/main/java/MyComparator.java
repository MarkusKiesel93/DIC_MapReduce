import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import java.io.Serializable;

public class MyComparator extends WritableComparator implements Serializable {

    MyComparator() {
        super(MyKey.class, true);
    }

    @Override
    public int compare(WritableComparable comparableA, WritableComparable comparableB) {
        MyKey keyA = (MyKey) comparableA;
        MyKey keyB = (MyKey) comparableB;

        return keyA.getToken().compareTo(keyB.getToken());
    }
}