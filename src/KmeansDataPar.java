import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

import mpi.*;

public class KmeansDataPar {
    public DataPoint[] centroids;
    public int numGroup;
    public int dimension;
    ArrayList<DataPoint> indata;

    public KmeansDataPar(int numG, int d) {
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

    public static void main(String[] args) throws MPIException {
	MPI.Init(args);
	if (args.length != 3) {
	    System.out
		    .println("[Usage] java KmeansData <input data> <number of cluster> <dimension>");
	    return;
	}
	
	int myrank = MPI.COMM_WORLD.Rank();
	
	KmeansData kmd = new KmeansData(Integer.parseInt(args[1]),
		    Integer.parseInt(args[2]));
	if (myrank == 0) {
	    kmd.parse(args[0]); // parse input and store in the object
	    kmd.setIniCen(); // set initial seed centroid
	}
	System.out.println("Rank: " + myrank);
	if (myrank == 0) {
	    for (int i = 0; i < kmd.centroids.length; i++) 
		System.out.println(kmd.centroids[i].data[0]);
	    for (int send = 1; send < MPI.COMM_WORLD.Size(); send++)
		MPI.COMM_WORLD.Send(kmd.centroids, 0, kmd.centroids.length, MPI.OBJECT, send, 99);
	}
	else {
	    MPI.COMM_WORLD.Recv(kmd.centroids, 0, 2, MPI.OBJECT, 0, 99);
	    for (int i = 0; i < kmd.centroids.length; i++) 
		System.out.println(kmd.centroids[i].data[0]);
	}
	MPI.Finalize();
	//kmd.kmeanProcedure(); // do kmean procedure

    }
}
