import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CategoryTokenKey implements WritableComparable<CategoryTokenKey> {

    private final static Text N_TOKEN = new Text("N");
    private Text category;
    private Text token;

    public CategoryTokenKey(){
        category = new Text();
        token = new Text();
    }

    public CategoryTokenKey(Text category, Text token) {
        this.category = category;
        this.token = token;
    }

    public Text getCategory() {
        return category;
    }

    public Text getToken() {
        return token;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        category.write(out);
        token.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        category.readFields(in);
        token.readFields(in);
    }

    @Override
    public int compareTo(CategoryTokenKey key) {
        int compareValue = category.compareTo(key.getCategory());
        if (compareValue == 0) {
            // make sure N is always first in each category
            // then sort by token
            if (token.equals(N_TOKEN)) {
                compareValue = -1;
            } else if (key.getToken().equals(N_TOKEN)) {
                compareValue = 1;
            } else {
                compareValue = token.compareTo(key.getToken());
            }
        }
        return compareValue;   // sort ascending
    }
}