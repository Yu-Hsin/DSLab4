


public class KmeansDNA {
    public double calDistDNA(String[] v1, String[] v2) {
	double dist = 0;

	// need to ask TA whether this is suitable
	for (int i = 0; i < v1.length; i++) {
	    dist += v1[i].equals(v2[i]) ? 0 : 1;
	}

	return dist;
    }
}
