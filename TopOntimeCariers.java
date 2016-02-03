import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.util.*;
import java.util.Iterator;

public class TopOntimeCariers extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TopOntimeCariers(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("/aviation/tmp");
        fs.delete(tmpPath, true);

        Job jobA = Job.getInstance(conf, "Cariers Count");
        jobA.setOutputKeyClass(Text.class);
        jobA.setOutputValueClass(IntWritable.class);

        jobA.setMapperClass(CarierCountMap.class);
        jobA.setReducerClass(CarierCountReduce.class);

        FileInputFormat.setInputPaths(jobA, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobA, tmpPath);

        jobA.setJarByClass(TopOntimeCariers.class);
        jobA.waitForCompletion(true);

        Job jobB = Job.getInstance(conf, "Top Cariers");
        jobB.setOutputKeyClass(Text.class);
        jobB.setOutputValueClass(IntWritable.class);

        jobB.setMapOutputKeyClass(NullWritable.class);
        jobB.setMapOutputValueClass(TextArrayWritable.class);

        jobB.setMapperClass(TopCariersMap.class);
        jobB.setReducerClass(TopCariersReduce.class);
        jobB.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(jobB, tmpPath);
        FileOutputFormat.setOutputPath(jobB, new Path(args[1]));

        jobB.setInputFormatClass(KeyValueTextInputFormat.class);
        jobB.setOutputFormatClass(TextOutputFormat.class);

        jobB.setJarByClass(TopOntimeCariers.class);
        return jobB.waitForCompletion(true) ? 0 : 1;
    }


    public static class TextArrayWritable extends ArrayWritable {
        public TextArrayWritable() {
            super(Text.class);
        }

        public TextArrayWritable(String[] strings) {
            super(Text.class);
            Text[] texts = new Text[strings.length];
            for (int i = 0; i < strings.length; i++) {
                texts[i] = new Text(strings[i]);
            }
            set(texts);
        }
    }

    public static class CarierCountMap extends Mapper<Object, Text, Text, IntWritable> {

	private final static IntWritable one = new IntWritable(1);
	private Text carier = new Text();
	
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			try {
				String[] tokens = value.toString().split(",");
				carier = new Text(tokens[2].trim());
				//  10, 13
				Float dep_delay = Float.parseFloat(tokens[10].trim());
				Float arr_delay = Float.parseFloat(tokens[13].trim());
				if ((dep_delay == 0) && (arr_delay == 0)) {
					context.write(carier, one);
				}
				 	} catch (Exception e) {
					  carier = new Text("error");
					  context.write(carier, one);    	
				  }
			
		}
        
        
    }

    public static class CarierCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
	
		}
    }

    public static class TopCariersMap extends Mapper<Text, Text, NullWritable, TextArrayWritable> {
        Integer N;
        
        private TreeMap<Integer, TextArrayWritable> topDict = new TreeMap<Integer, TextArrayWritable>();

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            this.N = conf.getInt("N", 10);
        }

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
	    
			Integer count = Integer.parseInt(value.toString());
			String word = key.toString();
			String[] v = new String[]{key.toString(), value.toString()};
				topDict.put(count, new TextArrayWritable(v));
			
			if (topDict.size() > N) {
				topDict.remove(topDict.firstKey());
			}
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
			for ( TextArrayWritable word : topDict.values() ) {
			context.write(NullWritable.get(), word);
            }
        }
    }

    public static class TopCariersReduce extends Reducer<NullWritable, TextArrayWritable, Text, IntWritable> {
        Integer N;
        
        //Map<Text, Integer> topDict = new HashMap<Text, Integer>();
        private TreeMap<Integer, TextArrayWritable> topDict = new TreeMap<Integer, TextArrayWritable>();
        // TODO

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            this.N = conf.getInt("N", 10);
        }

        @Override
        public void reduce(NullWritable key, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {
            // TODO
    	    for ( TextArrayWritable value : values ) {
    		
    		Text[] tuple = (Text[]) value.toArray();
    		//String word = tuple[0].toString();
    		Integer count = Integer.parseInt(tuple[1].toString());
		String[] v = new String[]{tuple[0].toString(), tuple[1].toString()};
		topDict.put(count, new TextArrayWritable(v));

		if (topDict.size() > N) {
		    topDict.remove(topDict.firstKey());
		}
	    }
	    for ( TextArrayWritable t : topDict.values() ) {
		Text[] tuple = (Text[]) t.toArray();
		context.write( tuple[0] , new IntWritable(Integer.parseInt(tuple[1].toString())) );
	    }
        }
    }

}

// >>> Don't Change
class Pair<A extends Comparable<? super A>,
        B extends Comparable<? super B>>
        implements Comparable<Pair<A, B>> {

    public final A first;
    public final B second;

    public Pair(A first, B second) {
        this.first = first;
        this.second = second;
    }

    public static <A extends Comparable<? super A>,
            B extends Comparable<? super B>>
    Pair<A, B> of(A first, B second) {
        return new Pair<A, B>(first, second);
    }

    @Override
    public int compareTo(Pair<A, B> o) {
        int cmp = o == null ? 1 : (this.first).compareTo(o.first);
        return cmp == 0 ? (this.second).compareTo(o.second) : cmp;
    }

    @Override
    public int hashCode() {
        return 31 * hashcode(first) + hashcode(second);
    }

    private static int hashcode(Object o) {
        return o == null ? 0 : o.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Pair))
            return false;
        if (this == obj)
            return true;
        return equal(first, ((Pair<?, ?>) obj).first)
                && equal(second, ((Pair<?, ?>) obj).second);
    }

    private boolean equal(Object o1, Object o2) {
        return o1 == o2 || (o1 != null && o1.equals(o2));
    }

    @Override
    public String toString() {
        return "(" + first + ", " + second + ')';
    }
}
// <<< Don't Change
