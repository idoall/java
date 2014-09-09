package idoall.xunyoubao.tags;

import java.util.ArrayList;
import java.util.List;
import java.util.Collections;


public class UserBehavior {

	private int userid;// 记录用户Id
    private String username;// 记录用户名称
    private int score; //临时保存分数
    private List<BTag> behaviortags = new ArrayList<BTag>();// 记录用户行为的Tags列表
    

    
    
    
    private static UserBehavior parser(String line) {
        //System.out.println(line);
    	UserBehavior model = new UserBehavior();
        String[] arr = line.split(",");
        
        model.setUserId(arr[0]);
        model.setUserName(arr[1]);
        List<BTag>  tmp_tags =  new ArrayList<BTag>();
        
        for(int i = 2;i<arr.length;i++){ 
        	String[] temp = arr[i].split(":");
        	BTag btag = new BTag(temp[0], Integer.parseInt(temp[1]));
        	
        	tmp_tags.add(btag);
        }
        
        //排序
        Collections.sort(tmp_tags);
        
        model.setBehaviortags(tmp_tags);
        return model;
    }
    
    
	
    public int getUserId() {
        return userid;
    }

    public void setUserId(String gameid) {
        this.userid = Integer.parseInt(gameid);
    }
    
    public int getScore() {
        return score;
    }

    public void setScore(String score) {
        this.score = Integer.parseInt(score);
    }

    public String getUserName() {
        return username;
    }

    public void setUserName(String username) {
        this.username = username;
    }
    
    public List<BTag> getBehaviortags() {
        return this.behaviortags;
    }

    public void setBehaviortags(List<BTag> behaviortags) {
        this.behaviortags = behaviortags;
    }   

    public void addBehaviortags(BTag behaviortags) {
        this.behaviortags.add(behaviortags);
    }      
 
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("UserId:" + this.userid);
        sb.append("\t\tUserName:" + this.username);
        
       
        
        for(BTag tag:this.behaviortags){ 
        	sb.append("\n" + tag.getTagName() + "->" + tag.getScore());
        }
        return sb.toString();
    }
    
    public static void main(String args[]) {
        String line = "1,lion,必备游戏:20,消磨时间:30,跑酷:20,经典怀旧:30,关卡:20,HD高清:10,雷电:10,影视改编:10,三国:10,街机:10,中文精品:10,小巧耐玩:10,";
        System.out.println(parser(line));
    }
	
}




