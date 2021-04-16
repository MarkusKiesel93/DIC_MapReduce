import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CategoryAKey implements WritableComparable<CategoryAKey> {

    private Text category;
    private IntWritable A;

    public CategoryAKey(){
        category = new Text();
        A = new IntWritable();
    }

    public CategoryAKey(Text category, IntWritable A) {
        this.category = category;
        this.A = A;
    }

    public Text getCategory() {
        return category;
    }

    public IntWritable getA() {
        return A;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        category.write(out);
        A.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        category.readFields(in);
        A.readFields(in);
    }

    @Override
    public int compareTo(CategoryAKey key) {
        int compareValue = category.compareTo(key.getCategory());
        if (compareValue == 0) {
            // make sure category count is always first in each category
            // highest value is the number of times a category is listed
            compareValue = A.compareTo(key.getA());
        }
        return -1 * compareValue;   // sort descending
    }
}