import argparse
import csv, os, time
from pymongo import MongoClient

# Get command line arguments
parser = argparse.ArgumentParser(description='Load SNP and locus data')
parser.add_argument('--dev', action='store_true', help='Only load chromosome 21 for development testing')
parser.add_argument('--path', help='Path to chromosome data')
parser.add_argument('--db', type=str, help='MongoDB database name')
parser.add_argument('--ohost', type=str, help='MongoDB host')
parser.add_argument('--coll', type=str, help='MongoDB collection')

args = parser.parse_args()

# Set default variables
dev = False
databaseName = 'snp_research'
mongoHost = 'mongodb://localhost:27017/'
collectionName = 'snps'
path = ''

# Update any present from CLI
if args.dev: # If dev mode, only load chr 21
    dev = True
if args.path is not None: # If set, use as root path for chromosome data
    path = args.path
if args.db is not None: # If set, use as database name for MySQL and MongoDB
    databaseName = args.db
if args.ohost is not None: # MongoDB connection string
    mongoHost = args.ohost
if args.coll is not None: # MongoDB collection name
    collectionName = args.coll

# Open results file
resultsFile = open('results-mongo.txt', 'w')

# Data files
snpFilePath = 'snpData-chr{0}.txt'
lociFilePath = 'lociData-chr{0}.txt'

# Chromosome list
chromosomes = ["21"] # dev list

# If not in dev mode, iterate through all chromosomes
if dev is False:
    chromosomes = ["1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","X","Y","MT"] # complete list

# Create MongoDB and MySQL connections
mongoClient = MongoClient(mongoHost)
mongoDb = mongoClient[databaseName]
mongoCollection = mongoDb[collectionName]

# Dictionaries and arrays for SQL and MongoDB queries
documents = {}     # Dictionary for MongoDB SNP/loci documents

for curChr in chromosomes:
    print "Chromosome " + str(curChr)

    # Set file paths for current chromosome
    curSnpFilePath = snpFilePath.format(curChr)
    curLociFilePath = lociFilePath.format(curChr)

    if len(path) > 0:
        curSnpFilePath = path.rstrip('\\') + '\\' + curSnpFilePath
        curLociFilePath = path.rsplit('\\') + '\\' + curLociFilePath

    # Clear dictionaries for loading multiple chromosomes
    documents.clear()

    print "Chromosome " + str(curChr) + ". Reading SNP data"

    # Read in data from SNP file
    with open(curSnpFilePath,'r') as csvfile:
        data = csv.reader(csvfile,delimiter='\t')
        for row in data:
            if(len(row) == 3):
                hasSig = False
                if row[2] != '' and row[2] != 'untested':
                    hasSig = True
                documents[row[0]] = {"rsid":row[0], "chr":row[1], "has_sig":row[2], "loci":[]}

    # Data for reporting
    mysqlSnpTime = '-'

    print "Chromosome " + str(curChr) + ". Reading loci data."

    # Now that we have primary keys for each SNP, read in loci data
    with open(curLociFilePath,'r') as csvfile:
        data = csv.reader(csvfile,delimiter='\t')
        for row in data:
            if(len(row) == 4 and row[0] in documents):
                # Load loci in Mongo documents
                curDoc = documents[row[0]]
                if curDoc["loci"] is None:
                    curDoc["loci"] = [{"mrna_acc":row[1],"gene":row[2],"class":row[3]}]
                else:
                    curDoc["loci"] = curDoc["loci"].append({"mrna_acc":row[1],"gene":row[2],"class":row[3]})
                documents[row[0]] = curDoc

    # Data for reporting
    mongoDocuments = len(documents)
    mongoTime = '-'

    print "Starting to insert " + mongoDocuments + " documents"

    # Log start time for MongoDB inserts
    start = time.time()

    # Insert each document with SNP and loci data
    for v in documents.iteritems():
        mongoCollection.insert(v[1])

    # Log end time
    end=time.time()

    mongoTime = end-start
    print "\tMongoDB: " + str(mongoTime) + "s (" + str(mongoDocuments) +" documents)"

    results = curChr + "\t" + str(mongoTime) + "\t" + str(mongoDocuments)
    print results
    resultsFile.write(results)

resultsFile.close()

mongoClient.close()