import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

public class KmeansData {

    public DataPoint[] centroids;
    public int numGroup;
    public int dimension;
    ArrayList<DataPoint> indata;

    public KmeansData(int numG, int d) {
	numGroup = numG;
	dimension = d;
	centroids = new DataPoint[numG];
	indata = new ArrayList<DataPoint>();
    }

    /**
     * calclate the Euclidean distance between two vectors
     * 
     * @param v1
     *            vector1
     * @param v2
     *            vector2
     * @return the Euclidean distance
     */
    public double calDistPoint(double[] v1, double[] v2) {
	double dist = 0;

	for (int i = 0; i < v1.length; i++) {
	    dist += Math.pow((v1[i] - v2[i]), 2);
	}
	return Math.sqrt(dist);
    }

    /**
     * run k-means procedure
     */
    public void kmeanProcedure() {
	int iteration = 1;
	while (true) {
	    System.out.println("Iteration: " + iteration++);
	    DataPoint[] newCentroids = new DataPoint[centroids.length];

	    @SuppressWarnings("unchecked")
	    ArrayList<DataPoint>[] groupM = new ArrayList[centroids.length];

	    for (int i = 0; i < centroids.length; i++) {
		groupM[i] = new ArrayList<DataPoint>();
	    }

	    // update for each group
	    updateGroup(groupM);
	    // update the centroids
	    getNewCen(groupM, newCentroids);
	    // check convergence
	    if (isConverge(newCentroids)) {
		printResult(groupM);
		return;
	    }
	    // update the old centroids
	    centroids = newCentroids;
	}
    }

    /**
     * for each DataPoint, re-assign their group based on the new centroids
     * 
     * @param groupM
     */
    public void updateGroup(ArrayList<DataPoint>[] groupM) {
	// iterate all data points
	for (int i = 0; i < indata.size(); i++) {
	    double minDist = Double.MAX_VALUE;
	    int group = 0;
	    // iterate all centroid
	    for (int j = 0; j < centroids.length; j++) {
		double dist = 0;
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
    public void getNewCen(ArrayList<DataPoint>[] groupM,
	    DataPoint[] newCentroids) {
	for (int i = 0; i < groupM.length; i++) {
	    if (groupM[i].size() == 0) { // no points in this centroids!
		System.out.println("No points in this centroids!");
	    }
	    double[] tmp = new double[dimension];
	    DataPoint newC = new DataPoint(tmp);

	    for (int j = 0; j < groupM[i].size(); j++) {
		newC.add(groupM[i].get(j));
	    }
	    newC.divide((double) groupM[i].size());
	    newCentroids[i] = newC;
	}
    }

    /**
     * calculate the difference betweeen new centroids and old centroids and see
     * if they are similar enough to satifsy the stop criterion
     * 
     * @param newCentroids
     *            new centroids
     * @return if the k-mean procedure converges or not
     */
    public boolean isConverge(DataPoint[] newCentroids) {
	double diff = 0.0;
	for (int i = 0; i < newCentroids.length; i++) {
	    diff += calDistPoint(newCentroids[i].data, centroids[i].data);
	}
	diff /= (double) numGroup;
	System.out.println("centroid difference: " + diff);
	return diff < 0.00001;
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
		double[] dArr = new double[strArr.length];
		for (int i = 0; i < strArr.length; i++)
		    dArr[i] = Double.parseDouble(strArr[i]);
		DataPoint dp = new DataPoint(dArr);
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
    }

    public void printResult(ArrayList<DataPoint>[] group) {
	System.out.println("Finish Running K-Means!");
	System.out.println("Number of data in each clusters:");
	int total = 0;
	for (int i = 0; i < group.length; i++) {
	    System.out.println("Group " + (i + 1) + ": " + group[i].size());
	    total += group[i].size();
	}
	System.out.println("Total data: " + total);
    }

    public static void main(String[] args) {
	long startTime = System.currentTimeMillis();
	if (args.length != 3) {
	    System.out
		    .println("[Usage] java KmeansData <input data> <number of cluster> <dimension>");
	    return;
	}

	KmeansData kmd = new KmeansData(Integer.parseInt(args[1]),
		Integer.parseInt(args[2]));
	kmd.parse(args[0]); // parse input and store in the object
	kmd.setIniCen(); // set initial seed centroid
	kmd.kmeanProcedure(); // do kmean procedure
	long endTime = System.currentTimeMillis();
	long totalTime = endTime - startTime;
	System.out.println("Total runtime: " + totalTime + "(ms)");
    }
}
