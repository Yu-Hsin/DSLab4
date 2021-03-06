import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

import mpi.*;

public class KmeansDataPar {
    public static final int MAX_ITER = 5000;

    public DataPoint[] centroids;
    public int numGroup;
    public int dimension;
    public DataPoint[] indata = null;

    public KmeansDataPar(int numG) {
	numGroup = numG;
	centroids = new DataPoint[numG];
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
     * for each DataPoint in the DataPoint array, re-assign their group based on
     * the new centroids
     * 
     * @param dataPoints
     *            The array storing DataPoint objects.
     * @param start
     *            Update objects from start.
     * @param end
     * 		  Update objects to end.
     */
    public void updateGroup(DataPoint[] dataPoints, int start, int end) {
	for (int i = start; i < end; i++) {

	    double minDist = Double.MAX_VALUE;
	    int group = 0;

	    for (int j = 0; j < centroids.length; j++) {
		double dist = 0;
		// find the closest centroids
		if ((dist = calDistPoint(centroids[j].data, dataPoints[i].data)) < minDist) {
		    group = j;
		    minDist = dist;
		}
	    }

	    dataPoints[i].group = group;
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

    public boolean isConverge(DataPoint[] newCentroids, int num_cluster) {
	double diff = 0.0;
	for (int i = 0; i < newCentroids.length; i++) {
	    diff += calDistPoint(newCentroids[i].data, centroids[i].data);
	}
	diff /= (double) num_cluster;
	System.out.println("centroid difference: " + diff);
	return diff < 0.001;
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
	    int dataLen = 0;

	    while ((str = br.readLine()) != null)
		dataLen++;
	    br.close();
	    indata = new DataPoint[dataLen];

	    br = new BufferedReader(new FileReader(fnName));
	    int idx = 0;
	    while ((str = br.readLine()) != null) {
		String[] strArr = str.split(",");
		dimension = strArr.length;
		double[] dArr = new double[strArr.length];
		for (int i = 0; i < strArr.length; i++)
		    dArr[i] = Double.parseDouble(strArr[i]);
		DataPoint dp = new DataPoint(dArr);
		indata[idx++] = dp;
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
	    int idx = (int) (Math.random() * indata.length);
	    if (used.contains(idx))
		continue;
	    centroids[count++] = indata[idx];
	    used.add(idx);
	}
    }

    public void printResult(int[] group) {
	System.out.println("Finish Running K-Means!");
	System.out.println("Number of data in each clusters:");
	int total = 0;
	for (int i = 0; i < group.length; i++) {
	    System.out.println("Group " + (i + 1) + ": " + group[i]);
	    total += group[i];
	}
	System.out.println("Total data: " + total);
    }

    public static void main(String[] args) throws MPIException {
	MPI.Init(args);
	long startTime = System.currentTimeMillis();
	if (args.length != 2) {
	    System.out
	    .println("[Usage] java KmeansDataPar <input data> <number of cluster>");
	    MPI.Finalize();
	    return;
	}

	// name of current process
	int myrank = MPI.COMM_WORLD.Rank();
	// the value of k in "k"means
	int num_cluster = Integer.parseInt(args[1]);
	// number of total points
	int dataSize = 0;
	// being true until converge
	boolean[] running = new boolean[1];
	running[0] = true;


	/* 
	 * Initialization
	 * Read the input file, determine the total number of points and randomly choose initial condition
	 */
	KmeansDataPar kmd = new KmeansDataPar(Integer.parseInt(args[1]));
	kmd.parse(args[0]); // parse input and store in the object
	dataSize = kmd.indata.length;
	if (myrank == 0) kmd.setIniCen(); // set initial seed centroid

	/*
	 * Here determines the segments of every process
	 * Ex:  5000 data, 5 process
	 *     p0: start=0, end=1000; p1: start=1000, end=2000; ... 
	 */
	int segNum = dataSize / MPI.COMM_WORLD.Size();
	int start = myrank * segNum;
	int end = Math.min((myrank + 1) * segNum, dataSize);
	DataPoint[] sumBuffer = null;

	/* =================== Start k-means here =========================== */
	for(int iter = 0; iter < MAX_ITER; iter++) {

	    /* 1. Broadcast the latest centroids */
	    MPI.COMM_WORLD.Bcast(kmd.centroids, 0, num_cluster, MPI.OBJECT, 0);

	    /*
	     * Procedures are different among Master and Slaves here.
	     */
	    if (myrank == 0) {

		/* 2.(master) Update the group of its segment */
		kmd.updateGroup(kmd.indata, start, end);

		/*
		 * 3.(master) In this step, we reduce the inter-communication
		 *            by only transmitting the stats of segments.
		 *            That is, each process is responsible for a part of 
		 *            the whole data, and then compute the sum of coordinates
		 *            of each cluster and the amount of points classified
		 *            into each cluster. 
		 *            Finally, the master process only need to sum all of the 
		 *            stats rather than receiving a part of data points.
		 */
		sumBuffer = new DataPoint[num_cluster];
		for (int i = 0; i < num_cluster; i++) {
		    double[] initialVal = new double[kmd.dimension];
		    for (int j = 0; j < kmd.dimension; j++) initialVal[j] = 0.0;
		    sumBuffer[i] = new DataPoint(initialVal);
		}

		for (int i = start; i < end; i++) {
		    DataPoint dpoint = kmd.indata[i];
		    sumBuffer[dpoint.group].count++;
		    sumBuffer[dpoint.group].add(dpoint);
		}

		for (int i = 1; i < MPI.COMM_WORLD.Size(); i++) {
		    DataPoint[] slaveBuf = new DataPoint[num_cluster];
		    MPI.COMM_WORLD.Recv(slaveBuf, 0, num_cluster, MPI.OBJECT, i, 1);

		    for (int j = 0; j < num_cluster; j++) {
			sumBuffer[j].count += slaveBuf[j].count;
			sumBuffer[j].add(slaveBuf[j]);
		    }
		}
	    } else {
		/* 2.(slaves) Update the group of each segment */
		kmd.updateGroup(kmd.indata, start, end);

		/* 3.(slaves) Send the stats to master */
		sumBuffer = new DataPoint[num_cluster];
		for (int i = 0; i < num_cluster; i++) {
		    double[] initialVal = new double[kmd.dimension];
		    for (int j = 0; j < kmd.dimension; j++) initialVal[j] = 0.0;
		    sumBuffer[i] = new DataPoint(initialVal);
		}
		
		for (int i = start; i < end; i++) {
		    DataPoint dpoint = kmd.indata[i];
		    sumBuffer[dpoint.group].count++;
		    sumBuffer[dpoint.group].add(dpoint);
		}
		MPI.COMM_WORLD.Send(sumBuffer, 0, num_cluster, MPI.OBJECT, 0, 1);
	    }

	    /* 5.(master) The master updates the centroids */
	    if (myrank == 0) {
		DataPoint[] newCentroids = null;
		int[] centroidNum = new int[num_cluster];

		for (int i = 0; i < num_cluster; i++) {
		    sumBuffer[i].divide((double)sumBuffer[i].count);
		    centroidNum[i] = sumBuffer[i].count;
		}
		newCentroids = sumBuffer;

		/* Check if the current results already converge. */
		if (kmd.isConverge(newCentroids, num_cluster)) {
		    running[0] = false;
		    kmd.printResult(centroidNum);
		}
		kmd.centroids = newCentroids;
	    }

	    /* Sync all process, if the k-means converge, every process breaks together */
	    MPI.COMM_WORLD.Bcast(running, 0, 1, MPI.BOOLEAN, 0);
	    if (!running[0])
		break;

	}

	long endTime = System.currentTimeMillis();
	long totalTime = endTime - startTime;
	if (myrank == 0) System.out.println("Total runtime: " + totalTime + "(ms)");
	MPI.Finalize();

    }
}
