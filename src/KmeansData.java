import java.util.ArrayList;

public class KmeansData {

    public DataPoint[] centroids;
    public int numGroup;
    public int dimension;
    ArrayList<DataPoint> data;

    public KmeansData(int numG, int d) {
	numGroup = numG;
	dimension = d;
	centroids = new DataPoint[numG];
	data = new ArrayList<DataPoint>();
    }

    public double calDistPoint(double[] v1, double[] v2) {
	double dist = 0;

	for (int i = 0; i < v1.length; i++) {
	    dist += Math.pow((v1[i] - v2[i]), 2);
	}

	return Math.sqrt(dist);
    }

    public void kmeanProcedure() {

	while (true) {
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
	for (int i = 0; i < data.size(); i++) {
	    double minDist = Double.MAX_VALUE;
	    int group = 0;
	    // iterate all centroid
	    for (int j = 0; j < centroids.length; j++) {
		double dist = 0;
		// find the closest centroids
		if ((dist = calDistPoint(centroids[i].data, data.get(i).data)) < minDist) {
		    group = j;
		    minDist = dist;
		}
	    }
	    groupM[group].add(data.get(i));
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

	return true;
    }

    public void parse(String fnName) {

    }

    public void setIniCen() {

    }

    public static void main(String[] args) {
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

    }
}
