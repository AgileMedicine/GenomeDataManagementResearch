Genome Data Management Research
==============

Script to load SNP and locus data from tab-delimited files into MySQL and MongoDB for performance comparisons.

Prerequistites:
<ul>
  <li>Python</li>
  <li>
    MySQL
    <ul><li>mysql.connector</li></ul>
  </li>
  <li>
    MongoDB
    <ul><li>pymongo</li></ul>
  </li>
  <li>Tab-delimited data files</li>
</ul>


<pre>
  usage: loader.py [-h] [--dev] [--path PATH] [--method METHOD] [--db DB]
                 [--yhost YHOST] [--username USERNAME] [--password PASSWORD]
                 [--ohost OHOST] [--coll COLL]

  Load SNP and locus data

  optional arguments:
    -h, --help           show this help message and exit
    --dev                Only load chromosome 21 for development testing
    --path PATH          Path to chromosome data
    --method METHOD      Load type: M(y)SQL, M(o)ngoDB, or (b)oth
    --db DB              MySQL and MongoDB database name
    --yhost YHOST        MySQL host
    --username USERNAME  MySQL username
    --password PASSWORD  MySQL password
    --ohost OHOST        MongoDB host
    --coll COLL          MongoDB collection
</pre>
