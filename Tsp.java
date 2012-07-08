/* 
* The mapredue code to find the approximate shortest hamiltonian cyce in * a given graph. The closeness of the output path could be decide by the * user. This provision is provided because in most of the cases the most * optimal path is not needed. Now, this is done using monte-carlo method * by repeatedly sampling arbitrary paths and finding out their 
* lengths
*/




import java.io.IOException;
import java.util.*;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;//Mapper
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.*;

public class Tsp {
    public static void main(String[] args) throws IOException {
	if(args.length != 2) {
	    System.out.printf("wrong input format. The format is: ");
	    System.out.println("java Tsp  i/p file o/p file");
	    System.exit(-1);
	}
	JobConf conf = new JobConf(Tsp.class);
	conf.setJobName("Tsp");
	conf.setJarByClass(Tsp.class);
  	//((JobConf) conf.getConfiguration()).setJar("Tsp.jar");

	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(Text.class);

	conf.setMapperClass(TspMapper.class);
	conf.setCombinerClass(TspReducer.class);
	//conf.setReducerClass(TspReducer.class);	

	FileInputFormat.addInputPath(conf,new Path(args[0]));
	FileOutputFormat.setOutputPath(conf,new Path(args[1]));
		
	JobClient.runJob(conf);
    }
    
    
    public static class TspMapper extends MapReduceBase
	implements Mapper<LongWritable, Text, Text, Text> {
	private Text path = new Text();
	private Text pathvalue = new Text();
	private final static IntWritable one = new IntWritable(1);
	private int city;
	private int limit;
	private int m[][];

	int[] findCost() {
	    int a[] = new int[city];
	    int r = 0;
	    int totalcost = 0;
	    int previous = 0;
	    int rest = city - 1; 
	    int[] result = new int[city + 1];

	    for(int j = 0; j < city; j++) {//intitalizing flag array. 
		a[j] = 0;
	    }

	    a[0] = 1;
	    result[r++] = 0;

	    for(int j = 0; j < city -1; j++) {
		int choose = chooseNextElement(rest,a);
		int cost = costOfEdge(previous,choose);
		totalcost = totalcost + cost;
		result[r++] = choose;
		a[choose] = 1;
		rest--;
		previous = choose;
	    }

	    result[city] = totalcost;
	    return result;
	}

	int chooseNextElement(int rest, int[] a) {
	    Random rand = new Random();
	    double random = rand.nextDouble();
	    double check = 1.0/rest;
	    double temp = check;
	    int i, j;

	    for( i = 1, j = 1; i <= rest && j < city ; j++) {
		if(random <= check && a[j] == 0) {
		    break;
		} else if (a[j] == 1) {
		    continue;
		} else if (a[j] == 0) {
		    i++;
		    check = check + temp;
		}
	    }

	    return j;
	}

	int costOfEdge(int previous, int choose) {
	    return m[previous][choose];
	}

	public void map(LongWritable key,Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	    String str = value.toString();
	    StringTokenizer token = new StringTokenizer(str);
	    limit = Integer.parseInt(token.nextToken());
	    city = Integer.parseInt(token.nextToken());
	    String temp = new String();
	    m = new int[city][city];

	    for( int i = 0; i < city; i++) {
		for(int j = 0; j < city; j++) {
		    temp = token.nextToken();
		    if(i == j) {
			m[i][j] = Integer.MAX_VALUE;
		    }
		    else {
			m[i][j] = Integer.parseInt(temp);
		    }
		}
	    }
	   
	    for(int i = 0; i < limit; i++) {
		int[] result = findCost();
		String string1,string2;
		StringBuffer buffer = new StringBuffer();

		string1 = String.valueOf(result[city]);
		pathvalue.set(string1);

		string2 = String.valueOf(result);		
		for(int j = 0; j < city; j++) {
		    buffer.append(result[j]);
		    buffer.append(" ");
		}
		
		string2 =  buffer.toString();
		path.set(string2);
		output.collect(new Text(string1), new Text(string2));
	    }

	}
    }
    
    public static class TspReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> { 
	Text t1 = new Text();

	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	    String a;
    	    a = values.next().toString();
    	    output.collect(key,new Text(a));
	}
    }
}
