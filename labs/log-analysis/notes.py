

logs=sc.textFile("file:///home/glatard/Documents/teaching_concordia/big-data/course/labs/log-analysis/appliance")
logs.count()

# Filtering and counting

## With 1 log (filter, count)

1. How many lines are in the log? # count
>>> logs.count()

2. How many sessions were started for user root? # filter
>>> logs.filter(lambda x: re.match(".*Starting Session [0-9]* of user root",x)).count()

3. How many unique users? # map, distinct
>>> logs.filter(lambda x: re.match(".*Starting Session [0-9]* of user .*",x)).map(lambda x: x.split()[-1]).distinct().count()

4. How many sessions per user? # reduceByKey
>> logs.filter(lambda x: re.match(".*Starting Session [0-9]* of user .*",x)).map(lambda x: (x.split()[-1],1)).reduceByKey(lambda x,y: x+y).collect()

# Error analysis

5. Count error messages, i.e., lines that contain string "error" (case insensitive) 
>> logs.filter(lambda x: re.match(".*error.*",x,re.IGNORECASE)).count()

Write a function that returns (message,timestamp) from a string
def extract(line):
    s=line.split()
    message=""
    for x in range(3,len(s)):
        message+=s[x]+" "
    timestamp=s[0]+" "+s[1]+" "+s[2]
    return (message,timestamp)

def extract(line):
    s=line.split()
    message=""
    for x in range(3,len(s)):
        message+=s[x]+" "
    timestamp=s[0]+" "+s[1]+" "+s[2]
    return (message,1)

6. 10 most frequent error messages with count # sortByKey, take
>>  for x in logs.filter(lambda x: re.match(".*error.*",x,re.IGNORECASE)).map(lambda x: extract(x)).reduceByKey(lambda x,y: x+y).map(lambda (x,y):(y,x)).sortByKey(False).take(10):
...   print x[0],x[1]

## With 2 logs (union, intersection) 

>>> troy=sc.textFile("file:///home/glatard/Documents/teaching_concordia/big-data/course/labs/log-analysis/troy-sparta/troy")
>>> sparta=sc.textFile("file:///home/glatard/Documents/teaching_concordia/big-data/course/labs/log-analysis/troy-sparta/sparta")

7. How many unique users in both systems (union)
>> appliance.union(consider).filter(lambda x: re.match(".*Starting Session [0-9]* of user .*",x)).map(lambda x: x.split()[-1]).distinct()

8. List users who connected to both systems (intersection)
>> appliance.filter(lambda x: re.match(".*Starting Session [0-9]* of user .*",x)).map(lambda x: x.split()[-1]).distinct().intersection(consider.filter(lambda x: re.match(".*Starting Session [0-9]* of user .*",x)).map(lambda x: x.split()[-1]).distinct())

9. List users who connected to only 1 system and list this system

>> appliance_users.union(consider_users).groupByKey().filter(lambda x: len(x[1])==1).map(lambda x: (x[0],list(x[1]))).collect()

# Anonymization (saveAsTextFile)

10. Anonymize the log, i.e.,
* replace users by names such as "user-1", etc
* replace IP addresses

def anonymize(line):
  names=[("pgirard","apollo"),("glite","penthesilea"),("root","achille"), ("newk","troy"), ("monkeyman","sparta"), ("bourreau","ajax"), ("glatard","lampedo"), ("cbrain","apollo"), ("data-provider","eurypyle"), ("gdm","achille") ]
  for n in names:
      line=line.replace(n[0],n[1])
  return line

>> troy.map(lambda x: anonymize(x)).saveAsTextFile("file:///tmp/troy-anonymized")
>> sparta.map(lambda x: anonymize(x)).saveAsTextFile("file:///tmp/sparta-anonymized")


