import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<MyKey, IntWritable> {

    @Override
    public int getPartition(MyKey key, IntWritable value, int numPartitions) {
        return (key.getToken().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }

}