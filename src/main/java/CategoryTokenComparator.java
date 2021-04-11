import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import java.io.Serializable;

public class CategoryTokenComparator extends WritableComparator implements Serializable {

    CategoryTokenComparator() {
        super(CategoryTokenKey.class, true);
    }

    @Override
    public int compare(WritableComparable wcA, WritableComparable wcB) {
        CategoryTokenKey keyA = (CategoryTokenKey) wcA;
        CategoryTokenKey keyB = (CategoryTokenKey) wcB;

        return keyA.getCategory().compareTo(keyB.getCategory());
    }
}