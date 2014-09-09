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
import idoall.xunyoubao.tags.BTag;

/*
 * 对Tag进行筛选，以用户的Tag维度，区分出哪些游戏有多少分，结果如下格式
1#lion#10	保卫萝卜2:极地冒险#3,激流快艇2#34,百战天虫2:末日浩劫#44,鳄鱼小顽皮爱洗澡2#9,现代战争4:决战时刻#21,NBA 2K14#5,近地轨道防御3#40,GT赛车2#43,都市赛车5#42,空中打击#18,雷神2:暗黑世界#35,三国群英传1#31,空中打击#18,三国志无双战#7,孤胆车神:维加斯#26,六发左轮#36,植物大战僵尸2#10,水果忍者#11,史上最牛的游戏2#12,树叶精灵#17,忍者跳跃#39,布丁怪兽#13,愤怒的小鸟:星球大战2#4,捕鱼达人2#1,小小指挥官2之世界争霸#33
1#lion#20	割绳子2#16,狂野飙车8:极速凌云#28,真实赛车3#32,布丁怪兽#13,树叶精灵#17,小小指挥官2之世界争霸#33,三剑之舞#22,保卫萝卜2:极地冒险#3,激流快艇2#34,熊出没之熊大快跑#8,生化危机4#19,神偷奶爸:小黄人快跑#27,植物大战僵尸2#10,雷神2:暗黑世界#35,百战天虫2:末日浩劫#44,空中打击#18,史上最牛的游戏2#12,割绳子2#16,忍者跳跃#39,熊出没之熊大快跑#8,神偷奶爸:小黄人快跑#27,神庙逃亡2柳岩版#2,极品飞车17:最高通缉#25,捕鱼达人2#1,神庙逃亡2柳岩版#2,保卫萝卜2:极地冒险#3,愤怒的小鸟:星球大战2#4,鳄鱼小顽皮爱洗澡2#9,水果忍者#11,布丁怪兽#13
1#lion#30	狂野之血#30,都市赛车5#42,三剑之舞#22,捕鱼达人2#1,激流快艇2#34,开心消消乐#6,小小指挥官2之世界争霸#33,神偷奶爸:小黄人快跑#27,熊出没之熊大快跑#8,神庙逃亡2柳岩版#2,鳄鱼小顽皮爱洗澡2#9,极品飞车17:最高通缉#25,侠盗猎车手:圣安地列斯#24,火线指令2#38,生化危机4#19,空中打击#18,三国志无双战#7,百战天虫2:末日浩劫#44,愤怒的小鸟:星球大战2#4,忍者跳跃#39,布丁怪兽#13,史上最牛的游戏2#12,水果忍者#11,植物大战僵尸2#10,保卫萝卜2:极地冒险#3,现代战争4:决战时刻#21,GT赛车2#43,背刺高通版#29,真实赛车3#32,狂野飙车8:极速凌云#28,三国群英传1#31,神庙逃亡2柳岩版#2,孤胆车神:维加斯#26
2#guest#10	孤胆车神:维加斯#26,六发左轮#36,现代战争4:决战时刻#21,NBA 2K14#5,近地轨道防御3#40,GT赛车2#43,都市赛车5#42,空中打击#18,雷神2:暗黑世界#35,水果忍者#11,愤怒的小鸟:星球大战2#4,三剑之舞#22,都市赛车5#42,狂野之血#30,孤胆车神:维加斯#26,神庙逃亡2柳岩版#2,三国群英传1#31,狂野飙车8:极速凌云#28,真实赛车3#32,背刺高通版#29,GT赛车2#43,极品飞车17:最高通缉#25,侠盗猎车手:圣安地列斯#24,火线指令2#38,现代战争4:决战时刻#21,生化危机4#19,空中打击#18,三国志无双战#7,神庙逃亡2柳岩版#2,神偷奶爸:小黄人快跑#27,熊出没之熊大快跑#8,空中打击#18
2#guest#20	水果忍者#11,鳄鱼小顽皮爱洗澡2#9,愤怒的小鸟:星球大战2#4,保卫萝卜2:极地冒险#3,神庙逃亡2柳岩版#2,捕鱼达人2#1,树叶精灵#17,捕鱼达人2#1,小小指挥官2之世界争霸#33,保卫萝卜2:极地冒险#3,激流快艇2#34,百战天虫2:末日浩劫#44,鳄鱼小顽皮爱洗澡2#9,植物大战僵尸2#10,水果忍者#11,史上最牛的游戏2#12,忍者跳跃#39,布丁怪兽#13,愤怒的小鸟:星球大战2#4,忍者跳跃#39,割绳子2#16,史上最牛的游戏2#12,空中打击#18,百战天虫2:末日浩劫#44,雷神2:暗黑世界#35,植物大战僵尸2#10,神偷奶爸:小黄人快跑#27,生化危机4#19,熊出没之熊大快跑#8,激流快艇2#34,保卫萝卜2:极地冒险#3,三剑之舞#22,小小指挥官2之世界争霸#33,树叶精灵#17,布丁怪兽#13,极品飞车17:最高通缉#25,狂野飙车8:极速凌云#28,真实赛车3#32,割绳子2#16,布丁怪兽#13
2#guest#30	忍者跳跃#39,布丁怪兽#13,史上最牛的游戏2#12,水果忍者#11,植物大战僵尸2#10,保卫萝卜2:极地冒险#3,鳄鱼小顽皮爱洗澡2#9,神庙逃亡2柳岩版#2,熊出没之熊大快跑#8,神偷奶爸:小黄人快跑#27,小小指挥官2之世界争霸#33,开心消消乐#6,激流快艇2#34,捕鱼达人2#1,百战天虫2:末日浩劫#44,愤怒的小鸟:星球大战2#4
 * */
public class Step2 {
	
	public static class MapStep1 extends MapReduceBase implements Mapper<Object, Text, Text, Text> {
        private final static Text k = new Text();
        private final static Text v = new Text();
        
      //  @Override
        
		public void map(Object key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            String[] tokens = Recommend.DELIMITER.split(value.toString());
            
            
            
               
            if(tokens[tokens.length-1].charAt(0)=='2')
            {
            	//读取当前行中的所有用户 	
            	List<String> userlist = new ArrayList<String>();
            	for(int i=1; i<tokens.length;i++)
            	{
            		if(tokens[i].charAt(0)=='2')
            		{
            			userlist.add(tokens[i]);
            		}
            	}
            	
            	//循环用户列表
            	for(String user:userlist)
            	{           	
            		String[] userItem = user.split("#"); 
                	k.set(userItem[2]+"#"+userItem[1]+"#"+userItem[3]);//key 是用户Id+用户名称+Tag分数         
                	
	            	for(int i=1; i<tokens.length;i++)
	            	{
	            		//以用户做为k，所以有用户标识的不写入map
	            		if(tokens[i].charAt(0)!='2')
	            		{
		            		v.set(tokens[i].substring(2));//去除掉是游戏还是用户的标识
		            		output.collect(k,v);
		            		//System.out.print("\n"+k+"\t"+v);
	            		}
	            	}
            	}
            }
            
            //String itemID = tokens[1];
            //String pref = tokens[2];
            //k.set(itemID);
            //v.set(itemID + "##" + pref);
//            for(int i=2; i<tokens.length;i++)
//            {
//            	output.collect(new Text(tokens[i]),k);
//            }
            
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

	        String input = path.get("Step2Input");
	        String output = path.get("Step2Output");

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
