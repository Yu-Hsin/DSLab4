import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

import mpi.*;


public class KmeansDNAPar {
    public static final int MAX_ITER = 5000;
    
    public DNAPoint[] centroids;
    public int numGroup;
    public int dimension;
    public DNAPoint[] indata = null;
    
    public KmeansDNAPar(int numG) {
	numGroup = numG;
	centroids = new DNAPoint[numG];
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
    
    public void updateGroup(DNAPoint[] dataPoints, int offset) {
	for (int i = 0; i < offset; i++) {

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
	    indata = new DNAPoint[dataLen];
	    
	    br = new BufferedReader(new FileReader(fnName));
	    int idx = 0;
	    while ((str = br.readLine()) != null) {
		String[] strArr = str.split(",");
		dimension = strArr.length;
		char[] dArr = new char[strArr.length];
		for (int i = 0; i < strArr.length; i++)
		    dArr[i] = strArr[i].charAt(0);
		DNAPoint dp = new DNAPoint(dArr);
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
    
    public static void main(String[] args) throws MPIException {
	MPI.Init(args);
	if (args.length != 2) {
	    System.out
		    .println("[Usage] java KmeansData <input data> <number of cluster>");
	    MPI.Finalize();
	    return;
	}

	// name of current process
	int myrank = MPI.COMM_WORLD.Rank();
	// the value of k in "k"means
	int num_cluster = Integer.parseInt(args[1]);
	// number of total points
	int[] dataSize = new int[1];
	// being true until converge
	boolean[] running = new boolean[1];
	running[0] = true;

	/* 
	 * Initialization
	 * Read the input file, determine the total number of points and randomly choose initial condition
	 */
	KmeansDNAPar kmd = new KmeansDNAPar(Integer.parseInt(args[1]));
	if (myrank == 0) {
	    kmd.parse(args[0]); // parse input and store in the object
	    kmd.setIniCen(); // set initial seed centroid
	    dataSize[0] = kmd.indata.length;
	    
	    System.out.println(myrank + " " + dataSize[0]);
	}

	/* =================== Start EM here =========================== */
	for(int iter = 0; iter < MAX_ITER; iter++) {

	    /* 1. Send Centeriod and How Many Points */
	    MPI.COMM_WORLD.Bcast(kmd.centroids, 0, num_cluster, MPI.OBJECT, 0);
	    MPI.COMM_WORLD.Bcast(dataSize, 0, 1, MPI.INT, 0);

	    /* 2. Send Data Point Segments */
	    int segNum = dataSize[0] / MPI.COMM_WORLD.Size();
	    if (myrank == 0) {
		for (int i = 1; i < MPI.COMM_WORLD.Size(); i++) {
		    int start = i * segNum;
		    int end = Math.min((i + 1) * segNum, dataSize[0]);
		    MPI.COMM_WORLD.Send(kmd.indata, start, end - start,
			    MPI.OBJECT, i, 0);
		}

		/* 3. Update the group of each segment */
		kmd.updateGroup(kmd.indata, segNum);

		/* 4. Gather the updated centroids */
		for (int i = 1; i < MPI.COMM_WORLD.Size(); i++) {
		    int bufSize = (i + 1) * segNum > dataSize[0] ? dataSize[0]
			    % segNum : segNum;
		    DNAPoint[] slaveBuf = new DNAPoint[bufSize];

		    MPI.COMM_WORLD.Recv(slaveBuf, 0, bufSize, MPI.OBJECT, i, 1);

		    for (int j = 0; j < slaveBuf.length; j++) {
			kmd.indata[i * segNum + j] = slaveBuf[j];
		    }
		}
	    } else {
		int bufSize = (myrank + 1) * segNum > dataSize[0] ? dataSize[0]
			% segNum : segNum;
		DNAPoint[] slaveBuf = new DNAPoint[bufSize];

		MPI.COMM_WORLD.Recv(slaveBuf, 0, bufSize, MPI.OBJECT, 0, 0);

		/* 3. Update the group of each segment */
		kmd.updateGroup(slaveBuf, bufSize);

		/* 4. Send the updated centroids to master */
		MPI.COMM_WORLD.Send(slaveBuf, 0, bufSize, MPI.OBJECT, 0, 1);
	    }

	    /* 5. The master update the centroids */
	    if (myrank == 0) {
		DNAPoint[] newCentroids = new DNAPoint[num_cluster];
		int[][][] ATCGNum = new int[num_cluster][4][dataSize[0]];
		int[] centroidNum = new int[num_cluster];

		/*
		 * Add all the 
		 */
		for (DNAPoint dpoint : kmd.indata) {
		    for (int i = 0; i < dpoint.data.length; i++) {
			ATCGNum[dpoint.group][kmd.baseToIdx(dpoint.data[i])][i]++;
		    }
		    centroidNum[dpoint.group]++;
		}

		for (int i = 0; i < num_cluster; i++) {
		    char[] curData = new char[kmd.dimension];
		    for (int j = 0; j < kmd.dimension; j++) {
			int minIdx = 0;
			for (int k = 1; k < 4; k++) {
			    if (ATCGNum[i][k][j] > ATCGNum[i][minIdx][j]) minIdx = k;
			}
			
			curData[j] = kmd.idxToBase(minIdx);
		    }
		    
		    newCentroids[i] = new DNAPoint(curData);
		}


		if (kmd.isConverge(newCentroids)) {
		    running[0] = false;
		    kmd.printResult(centroidNum);
		}
		kmd.centroids = newCentroids;
	    }

	    MPI.COMM_WORLD.Bcast(running, 0, 1, MPI.BOOLEAN, 0);
	    if (!running[0])
		break;

	}
	
	MPI.Finalize();
    }
    
    
}
