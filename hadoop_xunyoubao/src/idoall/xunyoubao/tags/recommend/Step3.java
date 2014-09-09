package idoall.xunyoubao.tags.recommend;


import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import java.util.ArrayList;
import java.util.List;






import idoall.xunyoubao.hdfs.HdfsDAO;
//import idoall.xunyoubao.tags.recommend;


/*
 *计算游戏的Tag对应用户的总分数，格式如下：
 *GT赛车2#43#1#lion	40
GT赛车2#43#2#guest	20
NBA 2K14#5#1#lion	10
NBA 2K14#5#2#guest	10
三剑之舞#22#1#lion	50
三剑之舞#22#2#guest	30
 * */
public class Step3 {
	
	public static class MapStep1 extends MapReduceBase implements Mapper<Object, Text, Text, Text> {
        private final static Text k = new Text();
        private final static Text v = new Text();
        
      //  @Override
        
		public void map(Object key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            String[] tokens = Recommend.DELIMITER.split(value.toString());
            
            
            String user = tokens[0];
            String[] userInfo = user.split("#");
            for(int i=1; i<tokens.length;i++)
        	{
            	
            	k.set(tokens[i]+"#"+userInfo[0]+"#"+userInfo[1]);
            	v.set(userInfo[2]);
        		output.collect(k,v); 
        		System.out.print("\n"+k+"\t"+v);
        	}
               
            
        }
    }
	
	public static class ReduceStep1 extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
        private final static Text v = new Text();

     //   @Override
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            
        	//计算分数
            int count=0;
            while (values.hasNext()) {
                
                count += Integer.parseInt(values.next().toString());
            }
            
            v.set(String.valueOf(count));
            output.collect(key, v);           
        }
    }

	 public static void run(Map<String, String> path) throws IOException {
	 
		 	JobConf conf = Recommend.config();

	        String input = path.get("Step3Input");
	        String output = path.get("Step3Output");

	        HdfsDAO hdfs = new HdfsDAO(Recommend.HDFS, conf);

	        //hdfs.rmr(input);
	        //hdfs.mkdirs(input);
	        //hdfs.copyFile(path.get("data"), input);
	        //hdfs.copyFile(path.get("data1"), input);
	        hdfs.rmr(output);
	        
	        conf.setMapOutputKeyClass(Text.class);
	        conf.setMapOutputValueClass(Text.class);

	        conf.setOutputKeyClass(Text.class);
	        conf.setOutputValueClass(Text.class);

	        conf.setMapperClass(MapStep1.class);
	        //conf.setCombinerClass(ReduceStep1.class);
	        conf.setReducerClass(ReduceStep1.class);

	        conf.setInputFormat(TextInputFormat.class);
	        conf.setOutputFormat(TextOutputFormat.class);

	        FileInputFormat.setInputPaths(conf, new Path(input));
	        FileOutputFormat.setOutputPath(conf, new Path(output));

	        RunningJob job = JobClient.runJob(conf);
	        while (!job.isComplete()) {
	            job.waitForCompletion();
	        }
		 
	 }
	
}
