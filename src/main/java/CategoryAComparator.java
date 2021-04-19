import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import java.io.Serializable;

public class CategoryAComparator extends WritableComparator implements Serializable {

    // invoke constructor.super
    CategoryAComparator() {
        super(CategoryAKey.class, true);
    }

    // use CATEGORY for mapping to same reducer
    @Override
    public int compare(WritableComparable wcA, WritableComparable wcB) {
        CategoryAKey keyA = (CategoryAKey) wcA;
        CategoryAKey keyB = (CategoryAKey) wcB;

        return keyA.getCategory().compareTo(keyB.getCategory());
    }
}