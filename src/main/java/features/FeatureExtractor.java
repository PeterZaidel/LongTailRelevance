package features;

import writables.ReduceResult;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

public interface FeatureExtractor {
    public void setQueriesMap(HashMap<String, Integer> queriesMap);
    public void setUrlsMap(HashMap<String, Integer> urlsMap);

    public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException;
    public boolean filter_reduce(Text key, Text value);

    public void combiner_update(Text key, Text value, Reducer.Context context);
    public void combiner_final(Text key, Reducer.Context context) throws IOException, InterruptedException;

    public void reduce_update(Text key, Text value, Reducer.Context context);
    public ReduceResult reduce_final(Text key, Reducer.Context context) throws IOException, InterruptedException;
}


