import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class CategoryTokenPartitioner extends Partitioner<CategoryTokenKey, IntWritable> {

    @Override
    public int getPartition(CategoryTokenKey key, IntWritable value, int numPartitions) {
        return Math.abs(key.getCategory().hashCode() % numPartitions);
    }

}