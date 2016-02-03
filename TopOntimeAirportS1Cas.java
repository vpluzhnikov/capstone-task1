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

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.hadoop.ColumnFamilyOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.hadoop.ColumnFamilyInputFormat;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.ConfigHelper.*;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.hadoop.cql3.CqlConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlOutputFormat;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.util.*;
import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.nio.ByteBuffer;



public class TopOntimeAirportS1Cas extends Configured implements Tool {
	
	static final String KEYSPACE = "aviation";
    static final String OUTPUT_COLUMN_FAMILY = "q2_1";
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TopOntimeAirportS1Cas(), args);
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

        jobA.setMapperClass(AirportStatMap.class);
        jobA.setReducerClass(AirportStatReduce.class);

        FileInputFormat.setInputPaths(jobA, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobA, tmpPath);
        //FileOutputFormat.setOutputPath(jobA, new Path(args[1]));

        jobA.setJarByClass(TopOntimeAirportS1Cas.class);
        jobA.waitForCompletion(true);

        Job jobB = Job.getInstance(conf, "Top Cariers");
        jobB.setOutputKeyClass(Text.class);
        jobB.setOutputValueClass(Text.class);

        jobB.setMapOutputKeyClass(Text.class);
        jobB.setMapOutputValueClass(Text.class);

        jobB.setMapperClass(TopCariersMap.class);
        jobB.setReducerClass(TopCariersReduce.class);
        jobB.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(jobB, tmpPath);
        //FileOutputFormat.setOutputPath(jobB, new Path(args[1]));

        jobB.setInputFormatClass(KeyValueTextInputFormat.class);
        jobB.setOutputKeyClass(ByteBuffer.class);
        jobB.setOutputValueClass(List.class);
        ConfigHelper.setOutputKeyspace(jobB.getConfiguration(), KEYSPACE);
        ConfigHelper.setOutputColumnFamily(jobB.getConfiguration(), KEYSPACE, OUTPUT_COLUMN_FAMILY);
        ConfigHelper.setOutputInitialAddress(jobB.getConfiguration(), args[1]);
		ConfigHelper.setOutputPartitioner(jobB.getConfiguration(), "org.apache.cassandra.dht.Murmur3Partitioner");
        String query = "UPDATE " + KEYSPACE + "." + OUTPUT_COLUMN_FAMILY + " SET carier = ?, fcount = ? ";
        CqlConfigHelper.setOutputCql(jobB.getConfiguration(), query);
        jobB.setOutputFormatClass(CqlOutputFormat.class);

        jobB.setJarByClass(TopOntimeAirportS1Cas.class);
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

    public static class AirportStatMap extends Mapper<Object, Text, Text, IntWritable> {

	private final static IntWritable one = new IntWritable(1);
		private Text mkey = new Text();

	@Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			try {
				String[] tokens = value.toString().split(",");
				//carier = new Text(tokens[2].trim());
				//airport = new Text(tokens[2].trim());
				mkey = new Text(tokens[4].trim() + ";" + tokens[2].trim());
				Float dep_delay = Float.parseFloat(tokens[10].trim());
				if (dep_delay == 0) {
					context.write(mkey, one);
				}
				 	} catch (Exception e) {
					  mkey = new Text("error;error");
					  context.write(mkey, one);    	
				  }
			
		}
        
        
    }

    public static class AirportStatReduce extends Reducer<Text, IntWritable, Text, Text> {
    
    	private Text carier = new Text();
    	private Text airport = new Text();
    	@Override
    
    	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			String[] tokens = key.toString().split(";");
			airport = new Text(tokens[0].trim());
			carier = new Text(tokens[1].trim());
			context.write(airport, new Text(carier + ";" + Integer.toString(sum)));
	
		}
    }

    public static class TopCariersMap extends Mapper<Text, Text, Text, Text> {
        Integer N;

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        	context.write(key, value);
        }

    }

    public static class TopCariersReduce extends Reducer<Text, Text, Map<String, ByteBuffer>, List<ByteBuffer>> {
        Integer N;
        private HashMap<Text, TreeMap<Integer, TextArrayWritable>> bigStat = new HashMap<Text, TreeMap<Integer, TextArrayWritable>>();
        private TreeMap<Integer, TextArrayWritable> topDict; 
        private Text airport = new Text();
        private Text carier_name = new Text();
        
        //topDict
        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            this.N = conf.getInt("N", 10);
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	
        	for ( Text value : values ) {
        		String[] tokens = value.toString().split(";");
        		Integer count = Integer.parseInt(tokens[1]);
        		String carier_name = tokens[0];
        		airport = key;
        		String[] v = new String[]{airport.toString(), carier_name, count.toString()};
        		topDict = new TreeMap<Integer, TextArrayWritable>();
        		topDict = bigStat.get(airport);
        		if (topDict == null) {
        			topDict = new TreeMap<Integer, TextArrayWritable>();
        		}  
        		topDict.put(count, new TextArrayWritable(v));
        		if (topDict.size() > N) {
        			topDict.remove(topDict.firstKey());
        		}
        		bigStat.put(airport, topDict);
        		
        	}

	    }
	    
	    @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
			for ( Map.Entry<Text, TreeMap<Integer,TextArrayWritable>> entry : bigStat.entrySet() ) {
				airport = entry.getKey();
				topDict = new TreeMap<Integer, TextArrayWritable>();
				topDict = entry.getValue();
				NavigableSet<Integer> nset = topDict.navigableKeySet();
				Iterator iter = nset.descendingIterator();
        		while (iter.hasNext()) {
				//for ( TextArrayWritable el : nset.values() ) {
        			TextArrayWritable el = topDict.get(iter.next());
					Text[] tuple = (Text[]) el.toArray();
					airport = tuple[0];
					//context.write(airport, new Text("Carier: "+tuple[1]+", Flight count: "+tuple[2]));
					Map<String, ByteBuffer> keys = new LinkedHashMap<String, ByteBuffer>();
					keys.put("airport", ByteBufferUtil.bytes(airport.toString()));
					List<ByteBuffer> variables = new ArrayList<ByteBuffer>();
					variables.add(ByteBufferUtil.bytes(tuple[1].toString()));
					variables.add(ByteBufferUtil.bytes(tuple[2].toString()));
					context.write( keys, variables );
				}
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
