import java.io.Serializable;



public class DNAPoint implements Serializable{
    
    private static final long serialVersionUID = 1L;
    public char [] data;
    public int group;
    
    public DNAPoint (char [] input) {
	data = input.clone();
    }
    
    public void setGroup (int g) { group = g; }
    public int  getGroup () { return group; }
    
}
