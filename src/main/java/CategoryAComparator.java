import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import java.io.Serializable;

public class CategoryAComparator extends WritableComparator implements Serializable {

    CategoryAComparator() {
        super(CategoryAKey.class, true);
    }

    @Override
    public int compare(WritableComparable wcA, WritableComparable wcB) {
        CategoryAKey keyA = (CategoryAKey) wcA;
        CategoryAKey keyB = (CategoryAKey) wcB;

        return keyA.getCategory().compareTo(keyB.getCategory());
    }
}