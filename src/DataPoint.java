import java.io.Serializable;



public class DataPoint implements Serializable{
    
    private static final long serialVersionUID = 1L;
    public double [] data;
    public int group;
    
    public int count;
    
    public DataPoint (double [] input) {
	data = input.clone();
	count = 0;
    }
    
    public void setGroup (int g) { group = g; }
    
    public int  getGroup () { return group; }
    
    public void add (DataPoint a) {
	for (int i = 0; i <data.length; i++) {
	    this.data[i] += a.data[i];
	}
    }
    
    public void divide (double d) {
	for (int i = 0; i < data.length; i++) {
	    this.data[i] /= d;
	}
    }
    
   
    
}
