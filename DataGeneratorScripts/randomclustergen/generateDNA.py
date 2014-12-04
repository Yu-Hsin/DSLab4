import sys
import csv
import getopt
import math
import random

def usage():
    print '$> python generateDNA.py <required args> [optional args]\n' + \
        '\t-c <#>\t\tNumber of clusters to generate\n' + \
        '\t-p <#>\t\tNumber of strands per cluster\n' + \
        '\t-o <file>\tFilename for the output of the raw data\n' + \
        '\t-l [#]\t\tLength of generated strands\n'  

       
       

def distance(s1, s2):
    '''
    Takes two DNA strands and computes the distance between them.
    '''
    return len(set(s1) & set(s2))

def tooClose(strand, strands, minDist):
    '''
    Computes the distance between the DNA strand and all strands
    in the list, and if any strands in the list are closer than minDist,
    this method returns true.
    '''
    for dna in strands:
        if distance(strand, dna) < minDist:
                return True

    return False

def handleArgs(args):
    # set up return values
    numClusters = -1
    numPoints = -1
    output = None
    strandLength = 10

    try:
        optlist, args = getopt.getopt(args[1:], 'c:p:l:o:')
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
        elif key == '-l':
            strandLength = int(val)

    # check required arguments were inputted  
    if numClusters < 0 or numPoints < 0 or \
            strandLength < 1 or \
            output is None:
        usage()
        sys.exit()
    return (numClusters, numPoints, output, \
            strandLength)

def getRandomBase():
    return random.choice(['G', 'T', 'A', 'C'])

def sameOrRandom(base, prob):
    if random.uniform(0, 1) < prob: # change it
        return getRandomBase()
    else: # keep it
        return base

def createStrand(strandLength):
    return [getRandomBase() for _ in range(strandLength)]

def generateStrand(centroid, prob):
    return [sameOrRandom(base, prob) for base in centroid]

# start by reading the command line
numClusters, \
numPoints, \
output, \
strandLength = handleArgs(sys.argv)

writer = csv.writer(open(output, "w"))

# step 1: generate each DNA centroid
centroids = []
minDistance = 3
for i in range(numClusters):
    newStrand = createStrand(strandLength)
    # is it far enough from the others?
    while (tooClose(newStrand, centroids, minDistance)):
        newStrand = createStrand(strandLength)
    centroids.append(newStrand)

# step 2: generate the points for each centroid
points = []
minClusterVar = 1
maxClusterVar = strandLength / 2
for centroid in centroids:
    # compute the variance for this cluster
    variance = random.randint(minClusterVar, maxClusterVar)
    print 'Centroid {0} with variance {1}'.format(centroid, variance)
    for j in range(0, numPoints):
        # generate a strand with expected variance from the centroid
        strand = generateStrand(centroid, 1.0 * variance / strandLength)
        # write the points out
        writer.writerow(strand)
