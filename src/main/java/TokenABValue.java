import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TokenABValue implements Writable {

    private Text token;
    private IntWritable A;
    private IntWritable B;

    public TokenABValue(){
        this.token = new Text();
        this.A = new IntWritable();
        this.B = new IntWritable();
    }

    public TokenABValue(Text token, IntWritable A, IntWritable B){
        this.token = token;
        this.A = A;
        this.B = B;
    }

    public Text getToken() {
        return token;
    }

    public IntWritable getA() {
        return A;
    }

    public IntWritable getB() {
        return B;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.token.readFields(in);
        this.A.readFields(in);
        this.B.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.token.write(out);
        this.A.write(out);
        this.B.write(out);
    }
}