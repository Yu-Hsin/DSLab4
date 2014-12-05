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
    public DataPoint[] indata = null;


    public KmeansDataPar(int numG, int d) {
	numGroup = numG;
	dimension = d;
	centroids = new DataPoint[numG];
    }

    public double calDistPoint(double[] v1, double[] v2) {
	double dist = 0;

	for (int i = 0; i < v1.length; i++) {
	    dist += Math.pow((v1[i] - v2[i]), 2);
	}
	return Math.sqrt(dist);
    }

    public void updateGroup(DataPoint[] dataPoints, int offset) {
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

    public boolean isConverge(DataPoint[] newCentroids, int num_cluster) {
	double diff = 0.0;
	for (int i = 0; i < newCentroids.length; i++) {
	    diff += calDistPoint(newCentroids[i].data, centroids[i].data);
	}
	diff /= (double) num_cluster;
	System.out.println(diff);
	return diff < 0.00001;
    }

    public void parse(String fnName) {
	try {
	    BufferedReader br = new BufferedReader(new FileReader(fnName));
	    String str = "";
	    int dataLen = 0;

	    while((str = br.readLine()) != null) dataLen++;
	    br.close();
	    indata = new DataPoint[dataLen];

	    br = new BufferedReader(new FileReader(fnName));
	    int idx = 0;
	    while ((str = br.readLine()) != null) {
		String[] strArr = str.split(",");
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

    public static void main(String[] args) throws MPIException {
	MPI.Init(args);
	if (args.length != 3) {
	    System.out
	    .println("[Usage] java KmeansDataPar <input data> <number of cluster> <dimension>");
	    return;
	}

	int myrank = MPI.COMM_WORLD.Rank();
	int num_cluster = Integer.parseInt(args[1]);
	int[] dataSize = new int[1];
	boolean[] running = new boolean[1];
	running[0] = true;

	KmeansDataPar kmd = new KmeansDataPar(Integer.parseInt(args[1]),
		Integer.parseInt(args[2]));
	if (myrank == 0) {
	    kmd.parse(args[0]); // parse input and store in the object
	    kmd.setIniCen(); // set initial seed centroid
	    dataSize[0] = kmd.indata.length;
	}
	//System.out.println("Rank: " + myrank);

	/* Start EM */

	while(true) {

	    /* 1. Send Centeriod and How Many Points*/
	    MPI.COMM_WORLD.Bcast(kmd.centroids, 0, num_cluster, MPI.OBJECT, 0);
	    MPI.COMM_WORLD.Bcast(dataSize, 0, 1, MPI.INT, 0);

	    /*
	    for (int i = 0; i < kmd.centroids.length; i++) 
		System.out.println(myrank + "/" + dataSize[0] + ":  " + kmd.centroids[i].data[0]);
	    */

	    /* 2. Send Data Point Segments */
	    int segNum = dataSize[0] / MPI.COMM_WORLD.Size();
	    if (myrank == 0) {
		for (int i = 1; i < MPI.COMM_WORLD.Size(); i++) {
		    int start = i*segNum;
		    int end = Math.min((i+1)*segNum, dataSize[0]);
		    MPI.COMM_WORLD.Send(kmd.indata, start, end-start, MPI.OBJECT, i, 0);
		}

		/* 3. Update the group of each segment */
		kmd.updateGroup(kmd.indata, segNum);

		/* 4. Gather the updated centroids */
		for (int i = 1; i < MPI.COMM_WORLD.Size(); i++) {
		    int bufSize = (i+1)*segNum > dataSize[0] ? dataSize[0]%segNum : segNum;
		    DataPoint[] slaveBuf = new DataPoint[bufSize];

		    MPI.COMM_WORLD.Recv(slaveBuf, 0, bufSize, MPI.OBJECT, i, 1);

		    for (int j = 0; j < slaveBuf.length; j++) {
			kmd.indata[i*segNum + j] = slaveBuf[j];
		    }
		}
	    }
	    else {
		int bufSize = (myrank+1)*segNum > dataSize[0] ? dataSize[0]%segNum : segNum;
		DataPoint[] slaveBuf = new DataPoint[bufSize];

		MPI.COMM_WORLD.Recv(slaveBuf, 0, bufSize, MPI.OBJECT, 0, 0);
		//System.out.println(myrank + " " + segNum + " " + slaveBuf.length);

		/* 3. Update the group of each segment */
		kmd.updateGroup(slaveBuf, bufSize);

		/* 4. Send the updated centroids to master */
		MPI.COMM_WORLD.Send(slaveBuf, 0, bufSize, MPI.OBJECT, 0, 1);
	    }

	    /* 5. The master update the centroids */
	    if (myrank == 0) {
		DataPoint[] newCentroids = new DataPoint[num_cluster];
		int[] centroidNum = new int[num_cluster];

		for (DataPoint dpoint : kmd.indata) {
		    if (newCentroids[dpoint.group] == null) newCentroids[dpoint.group] = new DataPoint(new double[]{0.0, 0.0});
		    newCentroids[dpoint.group].data[0] += dpoint.data[0];
		    newCentroids[dpoint.group].data[1] += dpoint.data[1];

		    centroidNum[dpoint.group]++;
		}


		for (int i = 0; i < num_cluster; i++) {
		    newCentroids[i].data[0] /= (double)centroidNum[i];
		    newCentroids[i].data[1] /= (double)centroidNum[i];
		}

		/*
		System.out.println("New centroids:");
		for (int i = 0; i < newCentroids.length; i++) System.out.print(newCentroids[i].data[0] + " " + newCentroids[i].data[1] + ";  ");
		System.out.println();
		*/

		if (kmd.isConverge(newCentroids, num_cluster)) {
		    running[0] = false;
		    System.out.println("converge!!!!!!");
		}
		kmd.centroids = newCentroids;
	    }

	    MPI.COMM_WORLD.Bcast(running, 0, 1, MPI.BOOLEAN, 0);
	    if (!running[0]) break;

	}

	MPI.Finalize();
	//kmd.kmeanProcedure(); // do kmean procedure

    }
}