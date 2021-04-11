import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MyKey implements WritableComparable<MyKey> {

    private Text category;
    private Text token;

    public MyKey(){
        this.category = new Text();
        this.token = new Text();
    }

    public MyKey (String category, String token) {
        this.category = new Text(category);
        this.token = new Text(token);
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
    public int compareTo(MyKey key) {
        return this.token.compareTo(key.getToken());
    }
}