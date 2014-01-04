import csv, os, time
import mysql.connector, pymongo
from mysql.connector import errorcode
from pymongo import MongoClient

# Open results file
resultsFile = open('results.txt', 'w')

# Data files
snpFilePath = 'snpData-chr{0}.txt'
lociFilePath = 'lociData-chr{0}.txt'

# Chromosome list
chromosomes = ["21"] # dev list
# chromosomes = ["1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","X","Y","MT"] # complete list

# Connection strings
mysqlConfig = {
    'user':'USERNAME',
    'password':'PASSWORD',
    'host':'127.0.0.1',
}
mysqlDatabase = 'snp_research'

mongoConnection = 'mongodb://localhost:27017/'
mongoDatabaseName = 'snp_research'
mongoCollectionName = 'snps'

# Create MongoDB and MySQL connections
mongoClient = MongoClient(mongoConnection)
mongoDb = mongoClient[mongoDatabaseName]
mongoCollection = mongoDb[mongoCollectionName]

# Create MySQL database, tables if not exists
mysqlConnection = mysql.connector.connect(**mysqlConfig)
createDbCursor = mysqlConnection.cursor()
try:
    mysqlConnection.database = mysqlDatabase
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_BAD_DB_ERROR:
        createDbCursor.execute("CREATE DATABASE {0} DEFAULT CHARACTER SET 'utf8'".format(mysqlDatabase))
        mysqlConnection.commit()
        mysqlConnection.database = mysqlDatabase
    else:
        print(err)
        exit(1)
        
TABLES = {}
TABLES['snp'] = (
    "CREATE TABLE IF NOT EXISTS `snp`("
    "  `id` int(11) NOT NULL AUTO_INCREMENT,"
    "  `rsid` varchar(45) NOT NULL,"
    "  `chr` varchar(5) NOT NULL,"
    "  `has_sig` binary(1) NOT NULL,"
    "  PRIMARY KEY (`id`)"
    ") ENGINE=InnoDB AUTO_INCREMENT=862719 DEFAULT CHARSET=utf8;")
TABLES['locus'] = (
    "CREATE TABLE IF NOT EXISTS `locus`("
    "  `id` int(11) NOT NULL AUTO_INCREMENT,"
    "  `mrna_acc` varchar(45) NOT NULL,"
    "  `gene` varchar(45) NOT NULL,"
    "  `class` varchar(45) NOT NULL,"
    "  `snp_id` int(11) NOT NULL,"
    "  PRIMARY KEY (`id`),"
    "  KEY `idx_snp_idx` (`snp_id`),"
    "  CONSTRAINT `idx_snp` FOREIGN KEY (`snp_id`) REFERENCES `snp` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION"
    ") ENGINE=InnoDB AUTO_INCREMENT=7564 DEFAULT CHARSET=utf8;")

for name, ddl in TABLES.iteritems():
    createDbCursor.execute(ddl)
    mysqlConnection.commit()

createDbCursor.close()

# Dictionaries and arrays for SQL and MongoDB queries
snpInserts = {}    # Dictionary for rsid/insert for SNP data
lociInserts = []   # Array for loci insert queries
rsidList = {}      # Dictionary of RSIDs that will also hold the 
                   # primary key for each SNP in SQL
documents = {}     # Dictionary for MongoDB SNP/loci documents

for curChr in chromosomes:
    print "Chromosome " + str(curChr)
    
    # Set file paths for current chromosome
    curSnpFilePath = snpFilePath.format(curChr)
    curLociFilePath = lociFilePath.format(curChr)
    
    # Clear dictionaries
    snpInserts.clear()
    lociInserts = []
    rsidList.clear()
    documents.clear()
    
    # Read in data from SNP file
    with open(curSnpFilePath,'r') as csvfile:
        data = csv.reader(csvfile,delimiter='\t')
        for row in data:
            if(len(row) == 3):        
                hasSig = False
                if row[2] != '' and row[2] != 'untested':
                    hasSig = True
                rsidList[row[0]] = 0
                insStr = "INSERT INTO snp (rsid, chr, has_sig) VALUES (\"{0}\", {1}, {2})".format(row[0], row[1], hasSig)
                snpInserts[row[0]] = insStr
                documents[row[0]] = {"rsid":row[0], "chr":row[1], "has_sig":row[2], "loci":[]}
    
    # Insert SNP data into MySQL
    mysqlCursor = mysqlConnection.cursor()
    
    # Log current run start time and number of SNPs
    snpEntries = len(snpInserts)
    start = time.time()
    
    # For each snp, insert record and then grab primary key
    for rsid,snp in snpInserts.iteritems():
        mysqlCursor.execute(snp)
        rsidList[rsid] = mysqlCursor.lastrowid
        
    # Commit all inserts to MySQL and grab end time
    mysqlConnection.commit()
    
    # Log completed time, close MySQL cursor
    end=time.time()
    mysqlSnpTime = end-start
    mysqlCursor.close()
    
    print "\tSNPs: " + str(mysqlSnpTime) + "s (" + str(snpEntries) + " records)"
    
    # Clear list of SNPs to free up memory
    snpInserts.clear()
    
    # Now that we have primary keys for each SNP, read in loci data
    with open(curLociFilePath,'r') as csvfile:
        data = csv.reader(csvfile,delimiter='\t')
        for row in data:
            if(len(row) == 4):
                if row[0] in rsidList and rsidList[row[0]] > 0:
                    insStr = "INSERT INTO locus (mrna_acc, gene, class, snp_id) VALUES (\"{0}\", \"{1}\", \"{2}\", {3})".format(row[1], row[2], row[3], rsidList[row[0]])
                    lociInserts.append(insStr)
                    curDoc = documents[row[0]]
                    if curDoc["loci"] is None:
                        curDoc["loci"] = [{"mrna_acc":row[1],"gene":row[2],"class":row[3]}]
                    else:
                        curDoc["loci"] = curDoc["loci"].append({"mrna_acc":row[1],"gene":row[2],"class":row[3]})
                    documents[row[0]] = curDoc
    
    # Create new cursor, enter loci data into MySQL
    cursor = mysqlConnection.cursor()
    
    # Log current run start time and number of loci
    lociEntries = len(lociInserts)
    start = time.time()
    
    # Insert each locus
    for locus in lociInserts:
        cursor.execute(locus)
    
    # Commit data to MySQL
    mysqlConnection.commit()
    
    # Log end time and total MySQL time
    end=time.time()
    
    mysqlLociTime = end-start
    print "\tLoci: " + str(mysqlLociTime) + "s (" + str(lociEntries) + " records)"
    
    mysqlTotalTime = mysqlSnpTime + mysqlLociTime
    print "\t\tTotal MySQL time: " + str(mysqlTotalTime) + "s"
    
    # Close MySQL cursor
    cursor.close()
    
    # Log start time for MongoDB inserts
    start = time.time()
    mongoDocuments = len(documents)
    
    # Insert each document with SNP and loci data
    for v in documents.iteritems():
        mongoCollection.insert(v[1])
        
    # Log end time
    end=time.time()
    
    mongoTime = end-start
    print "\tMongoDB: " + str(mongoTime) + "s (" + str(mongoDocuments) +" documents)"

    results = curChr + "\t" + str(mysqlSnpTime) + "\t" + str(snpEntries) + "\t" + str(mysqlLociTime) + "\t" + str(lociEntries) + "\t" + str(mongoTime) + "\t" + str(mongoDocuments)
    print results
    resultsFile.write(results)

resultsFile.close()
mysqlConnection.close()
mongoClient.close()