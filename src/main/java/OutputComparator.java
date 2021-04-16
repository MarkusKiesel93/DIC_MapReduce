import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.Serializable;

public class OutputComparator extends WritableComparator implements Serializable {

    OutputComparator() {
        super(OutputKey.class, true);
    }

    @Override
    public int compare(WritableComparable wcA, WritableComparable wcB) {
        OutputKey keyA = (OutputKey) wcA;
        OutputKey keyB = (OutputKey) wcB;

        return keyA.getCategory().compareTo(keyB.getCategory());
    }
}