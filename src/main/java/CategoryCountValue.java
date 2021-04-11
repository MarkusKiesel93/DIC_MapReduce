import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CategoryCountValue implements Writable {

    private Text category;
    private IntWritable count;

    public CategoryCountValue(){
        this.category = new Text();
        this.count = new IntWritable();
    }

    public CategoryCountValue(Text category, IntWritable count) {
        this.category = category;
        this.count = count;
    }

    public String getCategory() {
        return this.category.toString();
    }

    public int getCount() {
        return this.count.get();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.category.readFields(in);
        this.count.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.category.write(out);
        this.count.write(out);
    }
}