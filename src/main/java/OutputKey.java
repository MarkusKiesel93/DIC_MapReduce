import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OutputKey implements WritableComparable<OutputKey> {

    private Text category;
    private Text valueToken;

    public OutputKey(){
        category = new Text();
        valueToken = new Text();
    }

    public OutputKey(Text category, Text valueToken) {
        this.category = category;
        this.valueToken = valueToken;
    }

    public Text getCategory() {
        return category;
    }

    public Text getValueToken() {
        return valueToken;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        category.write(out);
        valueToken.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        category.readFields(in);
        valueToken.readFields(in);
    }

    @Override
    public String toString() {
        return category.toString() + ":" + valueToken.toString();
    }

    @Override
    public int compareTo(OutputKey key) {
        Text TOKEN = new Text("T");
        int compareValue;
        compareValue = category.compareTo(key.getCategory());

        // sort the special TOKEN category to last place
        if (compareValue != 0 && category.equals(TOKEN)) {
            compareValue = 1;
        }
        if (compareValue != 0 && key.getCategory().equals(TOKEN)) {
            compareValue = -1;
        }
        if (compareValue == 0) {
            if (category.equals(TOKEN)) {
                compareValue = valueToken.compareTo(key.getValueToken());
            } else {
                double value1 = Double.parseDouble(valueToken.toString());
                double value2 = Double.parseDouble(key.getValueToken().toString());
                if (value1 == value2) {
                    compareValue = 0;
                } else if (value1 > value2){
                    compareValue = -1;
                } else {
                    compareValue = 1;
                }
            }
        }
        return compareValue;
    }
}