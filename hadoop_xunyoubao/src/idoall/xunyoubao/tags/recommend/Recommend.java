package idoall.xunyoubao.tags.recommend;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.mapred.JobConf;

import idoall.xunyoubao.tags.recommend.Step1;
import idoall.xunyoubao.tags.recommend.Step2;

/*
 * 游戏原始数据格式：游戏Id，游戏名称，Tag，游戏时间
 * 用户行为原始数据格式：用户Id，用户名称，用户行业Tag
 * */
public class Recommend {
	
	public static final String HDFS = "hdfs://192.168.1.90:9000";
    public static final Pattern DELIMITER = Pattern.compile("[\t,]");
    
    public static void main(String[] args) throws Exception {
        Map<String, String> path = new HashMap<String, String>();
        path.put("data", "data/g_usergamebehavior.csv");
        path.put("data1", "data/g_games.csv");
        path.put("Step1Input", HDFS + "/user/root/xunyoubao");
        path.put("Step1Output", path.get("Step1Input") + "/step1");
        path.put("Step2Input", path.get("Step1Output"));
        path.put("Step2Output", path.get("Step1Input") + "/step2");
        path.put("Step3Input", path.get("Step2Output"));
        path.put("Step3Output", path.get("Step1Input") + "/step3");
        path.put("Step4Input", path.get("Step3Output"));
        path.put("Step4Output", path.get("Step1Input") + "/step4");
                

        Step1.run(path);//将游戏数据和用户行为数据进行组合
        Step2.run(path);//对Tag进行筛选，以用户的Tag维度，区分出哪些游戏有多少分
        Step3.run(path);//计算游戏的Tag对应用户的总分数
        Step4.run(path);//统计所属用户的游戏并且计算游戏的总Tag分数
 
        
        System.exit(0);
    }
    
    public static boolean isValidDate(String str) {
        boolean convertSuccess=true;
        //指定日期格式为四位年/两位月份/两位日期，注意yyyy/MM/dd区分大小写；
         SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd HH:mm");
         try {
        	 // 设置lenient为false. 否则SimpleDateFormat会比较宽松地验证日期，比如2007/02/29会被接受，并转换成2007/03/01
            format.setLenient(false);
            format.parse(str);
         } 
         catch (ParseException e) {
        	 // e.printStackTrace();
        	 // 如果throw java.text.ParseException或者NullPointerException，就说明格式不对
             convertSuccess=false;
         } 
         return convertSuccess;
  }
    
    public static JobConf config() {
        JobConf conf = new JobConf(Recommend.class);
        conf.setJobName("Recommand");
//        conf.addResource("classpath:/hadoop/core-site.xml");
//        conf.addResource("classpath:/hadoop/hdfs-site.xml");
//        conf.addResource("classpath:/hadoop/mapred-site.xml");
        conf.set("io.sort.mb", "1024");
        return conf;
    }

}
