import argparse
import csv, os, time
import MySQLdb

# Get command line arguments
parser = argparse.ArgumentParser(description='Load SNP and locus data')
parser.add_argument('--dev', action='store_true', help='Only load chromosome 21 for development testing')
parser.add_argument('--path', help='Path to chromosome data')
parser.add_argument('--db', type=str, help='MySQL database name')
parser.add_argument('--yhost', type=str, help='MySQL host')
parser.add_argument('--username', type=str, help='MySQL username')
parser.add_argument('--password', type=str, help='MySQL password')

args = parser.parse_args()

# Set default variables
dev = False
databaseName = 'snp_research'
username = 'dev'
password = ''
sqlHost = '127.0.0.1'
path = ''

# Update any present from CLI
if args.dev: # If dev mode, only load chr 21
    dev = True
if args.path is not None: # If set, use as root path for chromosome data
    path = args.path
if args.db is not None: # If set, use as database name for MySQL
    databaseName = args.db
if args.username is not None: # MySQL username
    username = args.username
if args.password is not None: # MySQL password
    password = args.password
if args.yhost is not None: # MySQL host name
    sqlHost = args.yhost

# Open results file
resultsFile = open('results-mysql.txt', 'w')

# Data files
snpFilePath = 'snpData-chr{0}.txt'
lociFilePath = 'lociData-chr{0}.txt'

# Chromosome list
chromosomes = ["21"] # dev list

# If not in dev mode, iterate through all chromosomes
if dev is False:
    chromosomes = ["1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","X","Y","MT"] # complete list

# Create MySQL database, tables if not exists
mysqlConnection = MySQLdb.connect(host=sqlHost,user=username,passwd=password)
createDbCursor = mysqlConnection.cursor()

createDbCursor.execute("CREATE DATABASE IF NOT EXISTS " + databaseName + " DEFAULT CHARACTER SET 'utf8'".format(databaseName))
mysqlConnection.commit()
mysqlConnection.close() # Reconnect with database name
mysqlConnection = MySQLdb.connect(host=sqlHost,user=username,passwd=password,db=databaseName)
createDbCursor = mysqlConnection.cursor()

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

for curChr in chromosomes:
    print "Chromosome " + str(curChr)
    
    # Set file paths for current chromosome
    curSnpFilePath = snpFilePath.format(curChr)
    curLociFilePath = lociFilePath.format(curChr)
    
    if len(path) > 0:
        curSnpFilePath = path.rstrip('\\') + '\\' + curSnpFilePath
        curLociFilePath = path.rsplit('\\') + '\\' + curLociFilePath
    
    # Clear dictionaries for loading multiple chromosomes
    snpInserts.clear()
    lociInserts = []
    rsidList.clear()

    print "Chromosome " + str(curChr) + ". Reading SNP Data"

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
    
    # Data for reporting
    snpEntries = len(snpInserts)    
    mysqlSnpTime = '-'
    
    # Insert SNP data into MySQL
    mysqlCursor = mysqlConnection.cursor()

    print "Chromosome " + str(curChr) + ". Inserting SNP Data."

    # Log current run start time
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

    print "Chromosome " + str(curChr) + ". Reading loci Data."

    # Now that we have primary keys for each SNP, read in loci data
    with open(curLociFilePath,'r') as csvfile:
        data = csv.reader(csvfile,delimiter='\t')
        for row in data:
            if(len(row) == 4):
                # Load loci in MySQL statements
                if row[0] in rsidList and rsidList[row[0]] > 0: # If RSID value is present, load with PK
                    insStr = "INSERT INTO locus (mrna_acc, gene, class, snp_id) VALUES (\"{0}\", \"{1}\", \"{2}\", {3})".format(row[1], row[2], row[3], rsidList[row[0]])
                    lociInserts.append(insStr)
                
    # Data for reporting
    lociEntries = len(lociInserts)
    mysqlLociTime = '-'
    mysqlTotalTime = '-'
    
    # Create new cursor, enter loci data into MySQL
    cursor = mysqlConnection.cursor()

    print "Chromosome " + str(curChr) + ". Inserting loci data."

    # Log current run start time and number of loci
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
    
    results = curChr + "\t" + str(mysqlSnpTime) + "\t" + str(snpEntries) + "\t" + str(mysqlLociTime) + "\t" + str(lociEntries) + "\t" + str(mysqlTotalTime)
    print results
    resultsFile.write(results)

resultsFile.close()

mysqlConnection.close()