package idoall.xunyoubao.tags;


import java.util.ArrayList;
import java.util.List;


public class GameTag {

	private int gameid;// 记录游戏Id
    private String gamename;// 记录游戏名称
    private List<String> tags = new ArrayList<String>();// 记录游戏的Tags列表
    
    
    private static GameTag parser(String line) {
        //System.out.println(line);
        GameTag game = new GameTag();
        String[] arr = line.split(",");
        
        game.setGameId(arr[0]);
        game.setGameName(arr[1]);
        List<String> tmp_tags = new ArrayList<String>();
        
        for(int i = 2;i<arr.length;i++){ 
        	tmp_tags.add(arr[i]);
        }
        game.setTags(tmp_tags);
        return game;
    }
	
    public int getGameId() {
        return gameid;
    }

    public void setGameId(String gameid) {
        this.gameid = Integer.parseInt(gameid);
    }

    public String getGameName() {
        return gamename;
    }

    public void setGameName(String gamename) {
        this.gamename = gamename;
    }
    
    public List<String> getTags() {
        return this.tags;
    }

    public void setTags(List<String> tags) {
        this.tags = tags;
    }
    
    public void addTags(String tags) {
        this.tags.add(tags);
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("GameId:" + this.gameid);
        sb.append("\t\tGameName:" + this.gamename);
        for(String tag:this.tags){ 
        	sb.append("\n" + tag);
        }
        return sb.toString();
    }
    
    public static void main(String args[]) {
        String line = "25,极品飞车17:最高通缉,经典怀旧,赛车,必备游戏,重力感应,大型游戏,EA";
        System.out.println(parser(line));
    }
    
}
