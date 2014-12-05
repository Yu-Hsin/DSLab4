import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

public class KmeansDNA {
    public DataPoint[] centroids;
    public int numGroup;
    public int dimension;
    ArrayList<DataPoint> indata;

    public KmeansDNA(int numG, int d) {
	numGroup = numG;
	dimension = d;
	centroids = new DataPoint[numG];
	indata = new ArrayList<DataPoint>();
    }

    public double calDistPoint(double[] v1, double[] v2) {
	double dist = 0;

	for (int i = 0; i < v1.length; i++) {
	    dist += Math.pow((v1[i] - v2[i]), 2);
	}
	return Math.sqrt(dist);
    }

    public void kmeanProcedure() {
	int iteration = 1;
	while (true) {
	    System.out.println(iteration++);
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
		System.out.println("Converge!");
		return;
	    }
	    // update the old centroids
	    centroids = newCentroids;
	}
    }

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

    public boolean isConverge(DataPoint[] newCentroids) {
	double diff = 0.0;
	for (int i = 0; i < newCentroids.length; i++) {
	    diff += calDistPoint(newCentroids[i].data, centroids[i].data);
	}
	diff /= (double) numGroup;
	System.out.println(diff);
	return diff < 0.00001;
    }

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

    public static void main(String[] args) {
	if (args.length != 3) {
	    System.out
		    .println("[Usage] java KmeansData <input data> <number of cluster> <dimension>");
	    return;
	}

	KmeansDNA kmdna = new KmeansDNA(Integer.parseInt(args[1]),
		Integer.parseInt(args[2]));
	kmdna.parse(args[0]); // parse input and store in the object
	kmdna.setIniCen(); // set initial seed centroid
	kmdna.kmeanProcedure(); // do kmean procedure

    }
}
