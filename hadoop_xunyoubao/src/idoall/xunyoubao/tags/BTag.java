package idoall.xunyoubao.tags;

public class BTag implements Comparable<BTag>{
    String tagname;        
    Integer score;
    
    public BTag(String tagname, int score)
    {
    	this.tagname = tagname;
    	this.score = score;
    }
    
    public String getTagName () {
    	return this.tagname; 
    }

    public void setTagName (String tagname) { 
    	this.tagname = tagname; 
    }

    public Integer getScore (){ 
    	return this.score; 
    } 

    public void setScore (int score) { 
    	this.score = score; 
    }

	@Override
	public int compareTo(BTag arg0) {
		if(this.score<arg0.score)return 1;
		else if(this.score==arg0.score)return 0;
		else return -1;
	} 
}