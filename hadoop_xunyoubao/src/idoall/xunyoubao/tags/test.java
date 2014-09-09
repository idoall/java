package idoall.xunyoubao.tags;

import idoall.xunyoubao.tags.recommend.Recommend;

import java.util.ArrayList;
import java.util.List;

public class test {

	public static void main(String[] args) throws Exception {
		//String str = "GT赛车2#43	1#经典怀旧,1#Gameloft,1#HD高清,1#大型游戏,1#赛车";
		String str = "lion#1	2#必备游戏:20,2#消磨时间:30,2#跑酷:20,2#经典怀旧:30,2#关卡:20,2#HD高清:10,2#雷电:10,2#影视改编:10,2#三国:10,2#街机:10,2#中文精品:10,2#小巧耐玩:10";
		String[] tokens = Recommend.DELIMITER.split(str.toString());
		String name = tokens[0];
		
		//获取用户或游戏的标识
		char relationtype = tokens[1].charAt(0);
		
		List<GameTag> gamelist = new ArrayList();
		List<UserBehavior> userlist = new ArrayList();
		
		 if ('1' == relationtype) {
			 String[] gamebaseinfo = tokens[0].split("#");//游戏基本信息
			 GameTag gametag = new GameTag();
			 gametag.setGameId(gamebaseinfo[1]);
			 gametag.setGameName(gamebaseinfo[0]);
			 for(int i=1; i<tokens.length;i++)
		     {
				 gametag.addTags(tokens[i].split("#")[1]);
		     }
			 
			 //向游戏列表中添加一个游戏
			 gamelist.add(gametag);
		 }
		 else{
			 String[] userbaseinfo = tokens[0].split("#");//用户基本信息
			 UserBehavior userbehavior = new UserBehavior();
			 userbehavior.setUserId(userbaseinfo[1]);
			 userbehavior.setUserName(userbaseinfo[0]);
			 for(int i=1; i<tokens.length;i++)
		     {
				 String[] tempArr = tokens[i].split("#")[1].split(":");				 
				 userbehavior.addBehaviortags(new BTag(tempArr[0], Integer.parseInt(tempArr[1])));;
		     }
			 
			 //向用户行为表中添加一个用户行为
			 userlist.add(userbehavior);
		 }
		 
		 System.out.println("\n游戏："+gamelist.size());
		 System.out.println("\n用户："+userlist.size());
		
	}
}
