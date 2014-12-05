import sys
import csv
import numpy
import getopt
import math
import random


def usage():
    print '$> python generaterawdata.py <required args> [optional args]\n' + \
        '\t-c <#>\t\tNumber of clusters to generate\n' + \
        '\t-p <#>\t\tNumber of points per cluster\n' + \
        '\t-o <file>\tFilename for the output of the raw data\n' + \
        '\t-v [#]\t\tMaximum coordinate value for points\n'  

       
       

def euclideanDistance(p1, p2):
    '''
    Takes two 2-D points and computes the Euclidean distance between them.
    '''
    diff = 0
    for i in range(len(p1)):
        if p1[i] != p2[i]:
           diff += 1
    return diff

def tooClose(point, points, minDist):
    '''
    Computes the euclidean distance between the point and all points
    in the list, and if any points in the list are closer than minDist,
    this method returns true.
    '''
    for pair in points:
        if euclideanDistance(point, pair) < minDist:
                return True

    return False

def handleArgs(args):
    # set up return values
    numClusters = -1
    numPoints = -1
    output = None
    maxValue = 10
    strandLen = 20 # default

    try:
        optlist, args = getopt.getopt(args[1:], 'c:p:v:o:l:')
    except getopt.GetoptError, err:
        print str(err)
        usage()
        sys.exit(2)

    for key, val in optlist:
        # first, the required arguments
        if   key == '-c':
            numClusters = int(val)
        elif key == '-p':
            numPoints = int(val)
        elif key == '-o':
            output = val
        # now, the optional argument
        elif key == '-v':
            maxValue = float(val)
	elif key == '-l':
	    strandLen = int(float(val))

    # check required arguments were inputted  
    if numClusters < 0 or numPoints < 0 or \
            maxValue < 1 or \
            output is None:
        usage()
        sys.exit()
    return (numClusters, numPoints, output, \
            maxValue, strandLen)

def drawOrigin(maxValue):
    return numpy.random.uniform(0, maxValue, 2)

def createStrand(length):
    return [genDNAbase() for x in range(length)]

def genDNAbase():
    return random.choice(['A','T','C','G'])

def gen_neighbor(centroid, num_mut):
    return [toChange(base, num_mut) for base in centroid]

def toChange(base, num_mut):
    prob = num_mut/strandLen 
    if random.uniform(0,1) < prob:
       return genDNAbase()
    else:
       return base
# start by reading the command line
numClusters, \
numPoints, \
output, \
maxValue,\
strandLen = handleArgs(sys.argv)


writer = csv.writer(open(output, "w"))

# step 1: generate each DNA centroid
DNA_centroid = []
minDistance = 5
for i in range(0, numClusters):
    #createStrand(strandLen)
    tmp_centroid = createStrand(strandLen)
    # is it far enough from the others?
    while (tooClose(tmp_centroid, DNA_centroid, minDistance)):
          tmp_centroid = drawOrigin(strandLen)
    DNA_centroid.append(tmp_centroid) 

# step 2: generate the points for each centroid
points = []
minClusterMut = 1
maxClusterMut = strandLen/2 # max: half of the bases are mutated
for i in range(0, numClusters):
    # compute the variance for this cluster
    num_mut = numpy.random.uniform(minClusterMut, maxClusterMut)
    cluster = DNA_centroid[i]
    for j in range(0, numPoints):
        # generate a 2D point with specified variance
        # point is normally-distributed around centroids[i]
        new_data = gen_neighbor(cluster, num_mut)
        # write the points out
        writer.writerow(new_data)
