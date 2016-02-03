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
import java.nio.ByteBuffer;
import com.google.common.base.CharMatcher;
import org.apache.commons.codec.digest.DigestUtils;
import java.util.LinkedHashMap;
import java.text.SimpleDateFormat;
import java.text.DateFormat;

public class Sightseeing extends Configured implements Tool {
	
	static final String KEYSPACE = "aviation";
    static final String OUTPUT_COLUMN_FAMILY = "q3_2";
        
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Sightseeing(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("/aviation/tmp");
        fs.delete(tmpPath, true);

        Job jobA = Job.getInstance(conf, "Airport Pair create");
        jobA.setOutputKeyClass(Text.class);
        jobA.setOutputValueClass(Text.class);

        jobA.setMapperClass(AirportCountMap.class);
        jobA.setReducerClass(AirportCountReduce.class);

        FileInputFormat.setInputPaths(jobA, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobA, tmpPath);
        jobA.setNumReduceTasks(14);


        jobA.setJarByClass(Sightseeing.class);
        jobA.waitForCompletion(true);
        
        Job jobB = Job.getInstance(conf, "Top Directions");
        jobB.setOutputKeyClass(Text.class);
        jobB.setOutputValueClass(Text.class);

        jobB.setMapOutputKeyClass(Text.class);
        jobB.setMapOutputValueClass(Text.class);

        jobB.setMapperClass(DirectionsMap.class);
        jobB.setReducerClass(DirectionsReduce.class);
        jobB.setNumReduceTasks(14);

        FileInputFormat.setInputPaths(jobB, tmpPath);
        jobB.setInputFormatClass(KeyValueTextInputFormat.class);
        
        ConfigHelper.setOutputKeyspace(jobB.getConfiguration(), KEYSPACE);
        ConfigHelper.setOutputColumnFamily(jobB.getConfiguration(), KEYSPACE, OUTPUT_COLUMN_FAMILY);
        ConfigHelper.setOutputInitialAddress(jobB.getConfiguration(), args[1]);
		ConfigHelper.setOutputPartitioner(jobB.getConfiguration(), "org.apache.cassandra.dht.Murmur3Partitioner");
				//(comp text PRIMARY KEY, origin text, odate text, otime text, connectionport text, ddate text, dtime text, destination text, delay text)
        String query = "UPDATE " + KEYSPACE + "." + OUTPUT_COLUMN_FAMILY + " SET origin = ? , odate = ? , otime = ? , connectionport = ? , ddate = ? , dtime = ? , destination = ? , delay = ? ";
        CqlConfigHelper.setOutputCql(jobB.getConfiguration(), query);
        jobB.setOutputFormatClass(CqlOutputFormat.class);
        
        
        jobB.setJarByClass(Sightseeing.class);
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

    public static class AirportCountMap extends Mapper<Object, Text, Text, Text> {
        List<String> stopWords;
        String delimiters;

	private final static IntWritable one = new IntWritable(1);
	private String airport = new String();
	private String destination = new String();
	private String delay = new String();
	private String depdate = new String();
	private String deptime = new String();
	DateFormat format = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	
			try {
				String[] tokens = value.toString().split(",");
				airport = tokens[4].trim();
				destination = tokens[6].trim();
				depdate = tokens[0].trim();
				deptime = tokens[8].trim();
				delay = tokens[12].trim();
				Integer dephour = Integer.parseInt((deptime.substring(1, 3)));
				if (dephour >= 12) {
					Date pdate = format.parse(depdate);
					long depdatems = pdate.getTime() - 172800000;
					pdate.setTime(depdatems);
					String newdepdate = format.format(pdate);
					context.write(new Text(airport+'/'+newdepdate), new Text(destination+"/"+depdate+"/"+deptime+"/"+delay+"/"+"2"));
				} else {
					context.write(new Text(destination+'/'+depdate), new Text(airport+"/"+depdate+"/"+deptime+"/"+delay+"/"+"1"));
				}
				  } catch (Exception e) {
					  airport = new String("error");
					  context.write(new Text(airport),new Text("Error")); 
				  }
			
		}
        
        
    }

    public static class AirportCountReduce extends Reducer<Text, Text, Text, Text> {

    	DateFormat format = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
    	@Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {	
        	//for (Text val : values) {
        	//	context.write(key, val);
        	//}
        	List<String[]> leg1Dict = new ArrayList<String[]>();
        	List<String[]> leg2Dict = new ArrayList<String[]>();
        	TreeMap<Float, TextArrayWritable> directions = new TreeMap<Float, TextArrayWritable>();	
        	//for (Text val : values) {
        	//	context.write(key, val);
        	//}
        	
        	try {
				for (Text val : values) {
					String[] tokens = new String(val.toString()+"/"+key.toString()).split("/");
					//tokens.add(key.toString());
					if (Integer.parseInt(tokens[4]) == 1) {
						//if (Float.parseFloat(tokens[3]) == 0) {
							leg1Dict.add(tokens);
						//}
					} else {
						//if (Float.parseFloat(tokens[3]) == 0) {
							leg2Dict.add(tokens);
						//}
					}
				}
				for (String[] leg1el : leg1Dict) {
					for (String[] leg2el : leg2Dict) {
								String[] v = new String[]{leg1el[0],leg1el[1],leg1el[2],leg2el[5],leg2el[1],leg2el[2],leg2el[0],leg2el[3]};
								directions.put(Float.parseFloat(leg2el[3]), new TextArrayWritable(v));
								if (leg1el[0] != leg2el[0]) {
									context.write(new Text(leg1el[0]+"/"+leg2el[5]+"/"+leg2el[0]+"/"+leg1el[1]), new Text(leg1el[0]+"/"+leg1el[1]+"/"+leg1el[2]+"/"+leg2el[5]+"/"+leg2el[1]+"/"+leg2el[2]+"/"+leg2el[0]+"/"+leg2el[3]));
								}
					}
				
				}	
			} catch (Exception e) {
				context.write(new Text("error"), new Text("error"));
				e.printStackTrace();
        	} 

		}
    }

    public static class DirectionsMap extends Mapper<Text, Text, Text, Text> {

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        	context.write(key, value); 			
		}
        
        
    }    
    
    public static class DirectionsReduce extends Reducer<Text, Text, Map<String, ByteBuffer>, List<ByteBuffer>> {
        Integer N;
        
        //Map<Text, Integer> topDict = new HashMap<Text, Integer>();
        private TreeMap<Float, TextArrayWritable> topDict = new TreeMap<Float, TextArrayWritable>();
        // TODO

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            this.N = conf.getInt("N", 1);
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	for ( Text value : values ) {
    	    	try {
					String[] tokens = value.toString().split("/");
					Float delay = Float.parseFloat(tokens[7]);
					topDict.put(delay, new TextArrayWritable(tokens));
					if (topDict.size() > N) {
						topDict.remove(topDict.lastKey());
					}
				} catch (Exception e) {
				e.printStackTrace();
				}  
			}
			for ( TextArrayWritable t : topDict.values() ) {
				Text[] direction = (Text[]) t.toArray();
				//context.write( direction[7] , new Text(direction[0].toString()+" "+direction[1].toString()+" "+direction[3].toString()+" "+direction[4].toString()+" "+direction[6].toString()));
				Map<String, ByteBuffer> keys = new LinkedHashMap<String, ByteBuffer>();
				keys.put("comp", ByteBufferUtil.bytes(direction[0].toString()+"/"+direction[3].toString()+"/"+direction[6].toString()+"/"+direction[1].toString()));
				List<ByteBuffer> variables = new ArrayList<ByteBuffer>();
				//(comp text PRIMARY KEY, origin text, odate text, otime text, connectionport text, ddate text, dtime text, destination text, delay text)
				variables.add(ByteBufferUtil.bytes(direction[0].toString()));
				variables.add(ByteBufferUtil.bytes(direction[1].toString()));
				variables.add(ByteBufferUtil.bytes(direction[2].toString()));
				variables.add(ByteBufferUtil.bytes(direction[3].toString()));
				variables.add(ByteBufferUtil.bytes(direction[4].toString()));
				variables.add(ByteBufferUtil.bytes(direction[5].toString()));
				variables.add(ByteBufferUtil.bytes(direction[6].toString()));
				variables.add(ByteBufferUtil.bytes(direction[7].toString()));
				context.write( keys, variables );
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


