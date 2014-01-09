import argparse
import csv, os, time
from pymongo import MongoClient
from result import Result
import gspread, getpass

# Get command line arguments
parser = argparse.ArgumentParser(description='Load SNP and locus data')
parser.add_argument('--dev', action='store_true', help='Only load chromosome 21 for development testing')
parser.add_argument('--path', help='Path to chromosome data')
parser.add_argument('--db', type=str, help='MongoDB database name')
parser.add_argument('--ohost', type=str, help='MongoDB host')
parser.add_argument('--coll', type=str, help='MongoDB collection')
parser.add_argument('--tag', type=str, help='Tag to place in results file')
parser.add_argument('--remote', action='store_true', help='Enable remote reporting')
parser.add_argument('--rkey', help='Google document key')
parser.add_argument('--start', type=str, help='Chromosome to start load from')
parser.add_argument('--bulk', action='store_true', help='Perform bulk insert.')

args = parser.parse_args()

# Set default variables
dev = False
remote = False
databaseName = 'snp_research'
mongoHost = 'mongodb://localhost:27017/'
collectionName = 'snps'
path = ''
tag = ''
docKey = ''
start = '1'
bulk = False

# Update any present from CLI
if args.dev: # If dev mode, only load chr 21
    dev = True
if args.remote and args.rkey is not None: # If set to remote log and document key is present, log to GDocs
    remote = True
    docKey = args.rkey
else:
    remote = False

if args.path is not None: # If set, use as root path for chromosome data
    path = args.path
if args.db is not None: # If set, use as database name for MySQL and MongoDB
    databaseName = args.db
if args.ohost is not None: # MongoDB connection string
    mongoHost = args.ohost
if args.coll is not None: # MongoDB collection name
    collectionName = args.coll
if args.tag is not None: # Tag to place in results file
    tag = args.tag
if args.start is not None:
    start = args.start
if args.bulk:
    bulk = True

# Open results file, print headers
resultsFileName = 'results-mongo'
if resultsFileName != "":
    resultsFileName += '-' + tag
resultsFileName += '.txt'
resultsFile = open(resultsFileName, 'w')
result = Result()
resultsFile.write(result.toHeader() + '\n')

if remote:
    gusername = raw_input("Enter Google username: ")
    gpassword = getpass.getpass("Enter Google password: ")    
    gs = gspread.Client(auth=(gusername,gpassword))
    gs.login()
    ss = gs.open_by_key(docKey)
    ws = ss.add_worksheet(tag + "-" + str(time.time()),1,1)
    ws.append_row(result.headerArr())

# Data files
snpFilePath = 'snpData-chr{0}.txt'
lociFilePath = 'lociData-chr{0}.txt'

# Chromosome list
chromosomes = ["21"] # dev list

# If not in dev mode, iterate through all chromosomes
if dev is False:
    chromosomes = ["1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","X","Y","MT"] # complete list
    if start != "1": # Allow restart from anywhere in chromosome list, sequentially as ordered above
        startList = []
        hitMin = False
        for cur in chromosomes:
            if cur == start:
                hitMin = True
            if hitMin:
                startList.append(cur)
        chromosomes = startList    

# Create MongoDB and MySQL connections
mongoClient = MongoClient(mongoHost)
mongoDb = mongoClient[databaseName]
mongoCollection = mongoDb[collectionName]

# Dictionaries and arrays for SQL and MongoDB queries
documents = {}     # Dictionary for MongoDB SNP/loci documents

for curChr in chromosomes:
    result = Result()
    result.method = "Mongo"
    result.tag = tag
    print "Chromosome " + str(curChr)
    result.chromosome = str(curChr)
    
    # Set file paths for current chromosome
    curSnpFilePath = snpFilePath.format(curChr)
    curLociFilePath = lociFilePath.format(curChr)

    if len(path) > 0:
        curSnpFilePath = path.rstrip('\\') + '\\' + curSnpFilePath
        curLociFilePath = path.rsplit('\\') + '\\' + curLociFilePath

    # Clear dictionaries for loading multiple chromosomes
    documents.clear()

    print "Chromosome " + str(curChr) + ". Reading SNP data"
    result.snpLoadStart = time.time()
    
    # Read in data from SNP file
    with open(curSnpFilePath,'r') as csvfile:
        data = csv.reader(csvfile,delimiter='\t')
        for row in data:
            if(len(row) == 3):
                hasSig = False
                if row[2] != '' and row[2] != 'untested':
                    hasSig = True
                documents[row[0]] = {"rsid":row[0], "chr":row[1], "has_sig":row[2], "loci":[]}

    result.snpLoadEnd = time.time()

    print "Chromosome " + str(curChr) + ". Reading loci data."
    result.lociLoadStart = time.time()
    
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
    result.lociLoadEnd = time.time()
    result.totalDocuments = len(documents)

    print "Starting to insert " + str(result.totalDocuments) + " documents"

    # Log start time for MongoDB inserts
    result.documentInsertStart = time.time()

    if bulk:
        print "Bulk insertion starting"
        mongoCollection.insert(documents)
    else:
        print "Individual document inserting starting"
        # Insert each document with SNP and loci data
        for v in documents.iteritems():
            mongoCollection.insert(v[1])

    # Log end time
    result.documentInsertEnd = time.time()
    result.calculate()
    
    print result.toTerm()
    resultsFile.write(result.toString() + '\n')
    if remote:
        print "Sending to GDocs..."
        gs.login()
        ws.append_row(result.stringArr())    

resultsFile.close()

mongoClient.close()