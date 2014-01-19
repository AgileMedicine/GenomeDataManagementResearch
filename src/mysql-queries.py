import argparse
import csv, os, time
import MySQLdb  # http://sourceforge.net/projects/mysql-python/
import result
from result import Result
import gspread, getpass # https://pypi.python.org/pypi/gspread/ (v0.1.0)

# Get command line arguments
parser = argparse.ArgumentParser(description='Load SNP and locus data')
parser.add_argument('--db', type=str, help='MySQL database name')
parser.add_argument('--yhost', type=str, help='MySQL host')
parser.add_argument('--username', type=str, help='MySQL username')
parser.add_argument('--password', type=str, help='MySQL password')
parser.add_argument('--tag', type=str, help='Tag to place in results file')
parser.add_argument('--remote', action='store_true', help='Enable remote reporting')
parser.add_argument('--rkey', help='Google document key')
args = parser.parse_args()

# Set default variables
remote = False
databaseName = 'snp_research'
username = 'dev'
password = ''
sqlHost = '127.0.0.1'
tag = ''
docKey = ''

# Update any present from CLI
if args.remote and args.rkey is not None: # If set to remote log and document key is present, log to GDocs
    remote = True
    docKey = args.rkey
else:
    remote = False
    
if args.db is not None: # If set, use as database name for MySQL
    databaseName = args.db
if args.username is not None: # MySQL username
    username = args.username
if args.password is not None: # MySQL password
    password = args.password
if args.yhost is not None: # MySQL host name
    sqlHost = args.yhost
if args.tag is not None: # Tag to place in results file
    tag = args.tag

# Open results file, print headers
resultsFileName = 'results-mysqlqueries'
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

# Create MySQL database, tables if not exists
mysqlConnection = MySQLdb.connect(host=sqlHost,user=username,passwd=password,db=databaseName)
cursor = mysqlConnection.cursor()

genes = ["ACSL6","ZDHHC8","TPH1","SYN2","DISC1","DISC2","COMT","FXYD6","ERBB4","DAOA","MEGF10","SLC18A1","DYM","SREBF2","NXRN1","CSF2RA","IL3RA","DRD2"]

for z in range(1,11):
    for g in genes:
        result = Result()
        result.method = "MySQL-QrySet" + str(z)
        result.tag = tag + "-" + g + "/" + str(z)
        print "Running queries: " + g + "/" + str(z)
    
        qryStart = time.time()
        cursor.execute("SELECT count(distinct s.rsid) FROM locus l, snp s WHERE l.snp_id = s.id AND l.gene = '" + g + "'")
        qryEnd = time.time()
        result.qryByGene = qryEnd-qryStart        
    
        qryStart = time.time()
        cursor.execute("SELECT count(distinct s.rsid) FROM locus l, snp s WHERE l.snp_id = s.id AND l.gene = '" + g + "' AND s.has_sig = true")
        qryEnd = time.time()
        result.qryByGeneSig = qryEnd-qryStart     

        resultsFile.write(result.toString() + '\n')
        if remote:
            try:
                print "Sending to GDocs..."
                gs.login()
                ws.append_row(result.stringArr()) 
            except:
                print "Unable to send to GDocs, continuing..."

print "Run complete!"