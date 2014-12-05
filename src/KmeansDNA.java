import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

public class KmeansDNA {

    public DNAPoint[] centroids;
    public int numGroup;
    public int dimension;
    ArrayList<DNAPoint> indata;

    public KmeansDNA(int numG) {
	numGroup = numG;
	centroids = new DNAPoint[numG];
	indata = new ArrayList<DNAPoint>();
    }

    /**
     * calclate the DNA distance between two vectors
     * 
     * @param v1
     *            vector1
     * @param v2
     *            vector2
     * @return the DNA distance
     */
    public int calDistPoint(char[] v1, char[] v2) {
	int dist = 0;
	;

	for (int i = 0; i < v1.length; i++) {
	    dist += v1[i] == v2[i] ? 0 : 1;
	}
	return dist;
    }

    public int baseToIdx(char base) {
	if (base == 'A')
	    return 0;
	else if (base == 'T')
	    return 1;
	else if (base == 'C')
	    return 2;
	else
	    return 3;
    }
    
    public char idxToBase (int idx) {
	if (idx == 0)
	    return 'A';
	else if (idx == 1)
	    return 'T';
	else if (idx == 2)
	    return 'C';
	else 
	    return 'G';
    }

    /**
     * run k-means procedure
     */
    public void kmeanProcedure() {
	int iteration = 1;
	while (true) {
	    System.out.println(iteration++);
	    DNAPoint[] newCentroids = new DNAPoint[centroids.length];

	    @SuppressWarnings("unchecked")
	    ArrayList<DNAPoint>[] groupM = new ArrayList[centroids.length];

	    for (int i = 0; i < centroids.length; i++) {
		groupM[i] = new ArrayList<DNAPoint>();
	    }

	    // update for each group
	    updateGroup(groupM);
	    // update the centroids
	    getNewCen(groupM, newCentroids);
	    // check convergence
	    if (isConverge(newCentroids) || iteration > 10000) {
		printResult(groupM);
		return;
	    }
	    // update the old centroids
	    centroids = newCentroids;
	}
    }
    
    
    public void printResult(ArrayList<DNAPoint>[] group) {
	System.out.println("Finish Running K-Means!");
	System.out.println("Number of data in each clusters:");
	int total = 0;
	for (int i = 0; i < group.length; i++) {
	    System.out.println("Group " + (i+1) + ": " + group[i].size());
	    /*for (int j = 0; j < group[i].size(); j++)
		System.out.println(group[i].get(j).data);
		*/
	    total += group[i].size();
	}
	System.out.println("Total data: " + total);
    }

    /**
     * for each DataPoint, re-assign their group based on the new centroids
     * 
     * @param groupM
     */
    public void updateGroup(ArrayList<DNAPoint>[] groupM) {
	// iterate all data points
	for (int i = 0; i < indata.size(); i++) {
	    int minDist = Integer.MAX_VALUE;
	    int group = 0;
	    // iterate all centroid
	    for (int j = 0; j < centroids.length; j++) {
		int dist = 0;
		// find the closest centroids
		if ((dist = calDistPoint(centroids[j].data, indata.get(i).data)) < minDist) {
		    group = j;
		    minDist = dist;
		}
	    }
	    groupM[group].add(indata.get(i));
	}
    }

    /**
     * get the new centroids from the newest formed groups
     * 
     * @param groupM
     *            a list where each element conatins a list of DataPoint belongs
     *            to that group
     * @param newCentroids
     *            new centroid
     */
    public void getNewCen(ArrayList<DNAPoint>[] groupM, DNAPoint[] newCentroids) {

	for (int i = 0; i < groupM.length; i++) {
	    if (groupM[i].size() == 0) { // no points in this centroids!
		System.out.println("No points in this centroids!");
	    }
	    char[] tmp = new char[dimension];
	    
	    for (int j = 0; j < dimension; j++) {
		int[] count = new int[4];
		for (int p = 0; p < groupM[i].size(); p++) {
		    char[] curDNA = groupM[i].get(p).data;
		    count[baseToIdx(curDNA[j])] += 1;
		}
		tmp[i] = idxToBase(getMaxIdx(count));
	    }
	    DNAPoint newC = new DNAPoint(tmp);
	    newCentroids[i] = newC;
	}
    }
    
    public int getMaxIdx (int [] input) {
	int ans = -1;
	int max = Integer.MIN_VALUE;
	for (int i = 0; i < input.length; i++) {
	    if (input[i] > max) {
		ans = i;
		max = input[i];
	    }
	}
	return ans;
    }
    
    /**
     * calculate the difference betweeen new centroids and old centroids and see
     * if they are similar enough to satifsy the stop criterion
     * 
     * @param newCentroids
     *            new centroids
     * @return if the k-mean procedure converges or not
     */
    public boolean isConverge(DNAPoint[] newCentroids) {
	double diff = 0;
	for (int i = 0; i < newCentroids.length; i++) {
	    diff += calDistPoint(newCentroids[i].data, centroids[i].data);
	}
	diff /= (double) numGroup;
	System.out.println(diff);
	return diff < 0.1; // should check
    }

    /**
     * parse the data and store them into a DataPoint array
     * 
     * @param fnName
     *            file name
     */
    public void parse(String fnName) {
	try {
	    BufferedReader br = new BufferedReader(new FileReader(fnName));
	    String str = "";
	    while ((str = br.readLine()) != null) {
		String[] strArr = str.split(",");
		dimension = strArr.length;
		char[] dArr = new char[strArr.length];
		for (int i = 0; i < strArr.length; i++)
		    dArr[i] = strArr[i].charAt(0);
		DNAPoint dp = new DNAPoint(dArr);
		indata.add(dp);
	    }
	    br.close();
	} catch (FileNotFoundException e) {
	    e.printStackTrace();
	} catch (IOException e) {
	    e.printStackTrace();
	}
    }

    /**
     * set initial centroids
     */
    public void setIniCen() {
	HashSet<Integer> used = new HashSet<Integer>();
	int count = 0;
	while (count != numGroup) {
	    int idx = (int) (Math.random() * indata.size());
	    if (used.contains(idx))
		continue;
	    centroids[count++] = indata.get(idx);
	    used.add(idx);
	}
	
	centroids[0] = indata.get(0);
	centroids[1] = indata.get(250);
	centroids[2] = indata.get(500);
	centroids[3] = indata.get(795);
	centroids[4] = indata.get(921);
    }

    public static void main(String[] args) {
	if (args.length != 2) {
	    System.out
		    .println("[Usage] java KmeansData <input da	`````````ta> <number of cluster>");
	    return;
	}

	KmeansDNA kmd = new KmeansDNA(Integer.parseInt(args[1]));
	kmd.parse(args[0]); // parse input and store in the object

	kmd.setIniCen(); // set initial seed centroid
	kmd.kmeanProcedure(); // do kmean procedure

    }
}
