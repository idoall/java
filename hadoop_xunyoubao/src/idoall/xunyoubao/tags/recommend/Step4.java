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
import java.util.Collections;
import java.util.List;









import idoall.xunyoubao.hdfs.HdfsDAO;
import idoall.xunyoubao.tags.BTag;


/*
 *统计所属用户的游戏并且计算游戏的总Tag分数，格式如下：
 *1#lion	GT赛车2#40,NBA 2K14#10,三剑之舞#50,三国志无双战#40,三国群英传1#40,侠盗猎车手:圣安地列斯#30,保卫萝卜2:极地冒险#80,六发左轮#10,割绳子2#40,史上最牛的游戏2#60,孤胆车神:维加斯#40,小小指挥官2之世界争霸#60,布丁怪兽#80,开心消消乐#30,忍者跳跃#60,愤怒的小鸟:星球大战2#60,捕鱼达人2#60,极品飞车17:最高通缉#50,树叶精灵#30,植物大战僵尸2#60,水果忍者#60,激流快艇2#60,火线指令2#30,熊出没之熊大快跑#70,狂野之血#30,狂野飙车8:极速凌云#50,现代战争4:决战时刻#40,生化危机4#50,百战天虫2:末日浩劫#60,真实赛车3#50,神偷奶爸:小黄人快跑#70,神庙逃亡2柳岩版#100,空中打击#70,背刺高通版#30,近地轨道防御3#10,都市赛车5#40,雷神2:暗黑世界#30,鳄鱼小顽皮爱洗澡2#60
 *2#guest	百战天虫2:末日浩劫#70,GT赛车2#20,植物大战僵尸2#70,NBA 2K14#10,背刺高通版#10,三剑之舞#30,水果忍者#80,三国志无双战#10,真实赛车3#30,三国群英传1#10,激流快艇2#70,侠盗猎车手:圣安地列斯#10,鳄鱼小顽皮爱洗澡2#70,保卫萝卜2:极地冒险#90,火线指令2#10,六发左轮#10,神偷奶爸:小黄人快跑#60,割绳子2#40,熊出没之熊大快跑#60,史上最牛的游戏2#70,近地轨道防御3#10,孤胆车神:维加斯#20,狂野之血#10,小小指挥官2之世界争霸#70,神庙逃亡2柳岩版#70,布丁怪兽#90,狂野飙车8:极速凌云#30,开心消消乐#30,雷神2:暗黑世界#30,忍者跳跃#70,现代战争4:决战时刻#20,愤怒的小鸟:星球大战2#80,空中打击#50,捕鱼达人2#70,生化危机4#30,极品飞车17:最高通缉#30,都市赛车5#20,树叶精灵#40
 * */
public class Step4 {
	
	public static class MapStep1 extends MapReduceBase implements Mapper<Object, Text, Text, Text> {
        private final static Text k = new Text();
        private final static Text v = new Text();
        
      //  @Override
        
		public void map(Object key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            String[] tokens = Recommend.DELIMITER.split(value.toString());
            
            
            String user = tokens[0];
            String[] baseInfo = user.split("#");
            k.set(baseInfo[2]+"#"+baseInfo[3]);//用户ID+Tag
        	v.set(baseInfo[0]+"#"+baseInfo[1]+"#"+tokens[1] );//游戏名称+游戏Id+分数
        	output.collect(k,v); 
        	System.out.print("\n"+k+"\t"+v);
        	
               
            
        }
    }
	
	public static class ReduceStep1 extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
        private final static Text v = new Text();

     //   @Override
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            
        	List<BTag> list = new ArrayList<BTag>();
        	
            while (values.hasNext()) {
            	String str =  values.next().toString();
            	BTag model = new BTag(str.split("#")[0]+"#"+str.split("#")[1],Integer.parseInt(str.split("#")[2]));
            	list.add(model);
               
            }
            Collections.sort(list);
            

        	StringBuilder sb = new StringBuilder();
            for(BTag temptag:list){
            	sb.append("," + temptag.getTagName()+"#"+temptag.getScore().toString());
            }
            // 
            v.set(sb.toString().replaceFirst(",", ""));
            output.collect(key, v);         
        }
    }

	 public static void run(Map<String, String> path) throws IOException {
	 
		 	JobConf conf = Recommend.config();

	        String input = path.get("Step4Input");
	        String output = path.get("Step4Output");

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
