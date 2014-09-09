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



import java.io.OutputStream;
import idoall.xunyoubao.hdfs.HdfsDAO;



public class Step1 {
	
	public static class MapStep1 extends MapReduceBase implements Mapper<Object, Text, Text, Text> {
		 

	      //  @Override
	        public void map(Object key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	            String[] tokens = Recommend.DELIMITER.split(value.toString());
	            String relationtype = new String();// 左右表标识
	            
	          
	            if(Recommend.isValidDate(tokens[tokens.length-1]))
	            {
	            	relationtype = "1";//表示游戏
	            }
	            else{
	            	relationtype = "2";//表示用户行为
	            }
	            for(int i=2; i<tokens.length;i++)
	            {
	            	if(!Recommend.isValidDate(tokens[i]))
	            	{
	            		//output.collect(new Text(tokens[1]+"#"+tokens[0]),new Text(relationtype+"#"+tokens[i]));
	            		if(relationtype=="2")
	            		{	//如果是用户
	            			//Tag 用户或是游戏类型标识#用户名称#用户Id#Tag分数
	            			output.collect(new Text(tokens[i].split(":")[0]),new Text(relationtype+"#"+tokens[1]+"#"+tokens[0]+"#"+tokens[i].split(":")[1]));
	            			//System.out.println("\n"+tokens[i].split(":")[0]+"\t"+relationtype+"#"+tokens[1]+"#"+tokens[0]+"#"+tokens[i].split(":")[1]);
	            		}
	            		else
	            		{
	            			//如果是游戏
	            			//Tag 用户或是游戏类型标识#游戏名称#游戏Id
	            			output.collect(new Text(tokens[i]),new Text(relationtype+"#"+tokens[1]+"#"+tokens[0]));
	            			//System.out.print("\n"+tokens[i]+"\t"+relationtype+"#"+tokens[1]+"#"+tokens[0]);
	            		}
	            	}
	            }
	            
	            //output.collect(k, v);
	        }
	    }
		
		public static class ReduceStep1 extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	        private final static Text v = new Text();

	     //   @Override
	        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	            StringBuilder sb = new StringBuilder();
	            while (values.hasNext()) {
	                sb.append("," + values.next());
	            }
	            
	            v.set(sb.toString().replaceFirst(",", ""));
	            output.collect(key, v);
	        }
	    }

		 public static void run(Map<String, String> path) throws IOException {
		 
			 	JobConf conf = Recommend.config();

		        String input = path.get("Step1Input");
		        String output = path.get("Step1Output");

		        HdfsDAO hdfs = new HdfsDAO(Recommend.HDFS, conf);

		        hdfs.rmr(input);
		        hdfs.mkdirs(input);
		        hdfs.copyFile(path.get("data"), input);
		        hdfs.copyFile(path.get("data1"), input);
		        
		        conf.setMapOutputKeyClass(Text.class);
		        conf.setMapOutputValueClass(Text.class);

		        conf.setOutputKeyClass(Text.class);
		        conf.setOutputValueClass(Text.class);

		        conf.setMapperClass(MapStep1.class);
		        //conf.setCombinerClass(CombinerStep1.class);
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
	
	
//	public static class MapStep1 extends MapReduceBase implements Mapper<Object, Text, Text, Text> {
// 
//
//      //  @Override
//        public void map(Object key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
//            String[] tokens = Recommend.DELIMITER.split(value.toString());
//            String relationtype = new String();// 左右表标识
//            
//          
//            if(Recommend.isValidDate(tokens[tokens.length-1]))
//            {
//            	relationtype = "1";//表示游戏
//            }
//            else{
//            	relationtype = "2";//表示用户行为
//            }
//            for(int i=2; i<tokens.length;i++)
//            {
//            	if(!Recommend.isValidDate(tokens[i]))
//            	{
//            		output.collect(new Text(tokens[1]+"#"+tokens[0]),new Text(relationtype+"#"+tokens[i]));
//            	}
//            }
//            
//            //output.collect(k, v);
//        }
//    }
//	
//	public static class ReduceStep1 extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
//        private final static Text v = new Text();
//
//     //   @Override
//        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
//            StringBuilder sb = new StringBuilder();
//            while (values.hasNext()) {
//                sb.append("," + values.next());
//            }
//            
//            v.set(sb.toString().replaceFirst(",", ""));
//            output.collect(key, v);
//        }
//    }
//
//	 public static void run(Map<String, String> path) throws IOException {
//	 
//		 	JobConf conf = Recommend.config();
//
//	        String input = path.get("Step1Input");
//	        String output = path.get("Step1Output");
//
//	        HdfsDAO hdfs = new HdfsDAO(Recommend.HDFS, conf);
//
//	        hdfs.rmr(input);
//	        hdfs.mkdirs(input);
//	        hdfs.copyFile(path.get("data"), input);
//	        hdfs.copyFile(path.get("data1"), input);
//	        
//	        conf.setMapOutputKeyClass(Text.class);
//	        conf.setMapOutputValueClass(Text.class);
//
//	        conf.setOutputKeyClass(Text.class);
//	        conf.setOutputValueClass(Text.class);
//
//	        conf.setMapperClass(MapStep1.class);
//	        //conf.setCombinerClass(CombinerStep1.class);
//	        conf.setReducerClass(ReduceStep1.class);
//
//	        conf.setInputFormat(TextInputFormat.class);
//	        conf.setOutputFormat(TextOutputFormat.class);
//
//	        FileInputFormat.setInputPaths(conf, new Path(input));
//	        FileOutputFormat.setOutputPath(conf, new Path(output));
//
//	        RunningJob job = JobClient.runJob(conf);
//	        while (!job.isComplete()) {
//	            job.waitForCompletion();
//	        }
//		 
//	 }
	
}
