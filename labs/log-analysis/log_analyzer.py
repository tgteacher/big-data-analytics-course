#!/usr/bin/env python

import argparse, os, re, sys
from pyspark import SparkContext, SparkConf

##############################################
############# Utility functions ##############
##############################################

def print_banner(i,message):
    print "\n* Q"+str(i)+": "+message

def print_result(machine,result):
    print "  + "+machine+": "+str(result)

##############################################


##############################################
##### The following functions implement  #####
##### the queries requested in the       #####
##### assignment.                        #####
##############################################

# Q1
def q1(all_pairs):
    print_banner(1, "line counts")
    for (log_name,log) in all_pairs:
        print_result(log_name,log.count())

## Q2
def q2(all_pairs):
    print_banner(2, "sessions of user 'achille'")
    for (log_name,log) in all_pairs:
        result = log.filter(lambda x:\
                            re.match(".*Starting Session .* of user achille",x))
        print_result(log_name,result.count())

## Q3
def q3(all_pairs):
    print_banner(3,"unique user names")
    for (log_name,log) in all_pairs:
        users = log.filter(lambda x:\
                           re.match(".*Starting Session .* of user .*",x))\
                   .map(lambda x: str(x.split()[-1])[:-1])\
                   .distinct()\
                   .collect()
        print_result(log_name,users)
                              
## Q4
def q4(all_pairs):
    print_banner(4,"sessions per user")
    for (log_name,log) in all_pairs:
        result = log.filter(lambda x:\
                        re.match(".*Starting Session .* of user .*",x))\
                    .map(lambda x: (str(x.split()[-1])[:-1],1) )\
                    .reduceByKey(lambda x,y: x+y)\
                    .collect()
        print_result(log_name,result)

## Q5
def q5(all_pairs):
    print_banner(5,"number of errors")
    for (log_name,log) in all_pairs:
        result=log.filter(lambda x: re.match(".*error.*",x,re.IGNORECASE)).count()
        print_result(log_name,result)

## Q6
def get_message(line):
    s=line.split()
    message=""
    for x in range(4,len(s)):
        message+=s[x]+" "
    return message

def q6(all_pairs):
    print_banner(6,"5 most frequent error messages")
    for (log_name,log) in all_pairs:
        result=log.filter(lambda x: re.match(".*error.*",x,re.IGNORECASE))\
                  .map(lambda x: (get_message(x),1))\
                  .reduceByKey(lambda x,y: x+y)\
                  .map(lambda (x,y):(y,str(x)))\
                  .sortByKey(False).take(5)
        sep="\n    - "
        print_result(log_name,sep+sep.join(map(str,result)))

## Q7
def get_users(log):
     return log.filter(lambda x: re.match(".*Starting Session .* of user .*",x))\
               .map(lambda x: str(x.split()[-1])[:-1])\
               .distinct()

def q7(all_pairs):
    print_banner(7,"users who started a session on both hosts.")
    (log_name1,log1)=all_pairs[0]
    (log_name2,log2)=all_pairs[1]
    result = get_users(log1).intersection(get_users(log2)).collect()
    print_result("",result)

## Q8
def q8(all_pairs):
    print_banner(8,"users who started a session on exactly one host, with host name.")
    (log_name1,log1)=all_pairs[0]
    (log_name2,log2)=all_pairs[1] 
    users1=get_users(log1).map(lambda x: (x,log_name1))
    users2=get_users(log2).map(lambda x: (x,log_name2))
    result=users1.union(users2)\
                 .groupByKey()\
                 .filter(lambda x: len(x[1])==1)\
                 .map(lambda x: (x[0],list(x[1])[0]))\
                 .collect()
    print_result("",result)

## Q9
def name_mapping(user_names):
    mapping = []
    count=0
    for name in sorted(user_names):
        mapping.append((name,"user-"+str(count)))
        count+=1
    return mapping
    
def anonymize(line,mapping):
  for n in mapping:
      line=line.replace(n[0],n[1])
  return line

def q9(all_pairs):
    print_banner(9,"anonymization")
    (log_name1,log1)=all_pairs[0]
    (log_name2,log2)=all_pairs[1]
    for (log_name,log) in [(log_name1,log1),(log_name2,log2)]:
        mapping = name_mapping(get_users(log).collect())
        file_name=log_name+"-anonymized"
        base_name=file_name
        count=0
        while(os.path.exists(file_name)):
            file_name=base_name+"-"+str(count)
            count+=1
        dest_url=os.path.join("file://"+os.getcwd(),file_name)
        log.map(lambda x: anonymize(x,mapping)).saveAsTextFile(dest_url)
        sep="\n.    "
        print_result(log_name,sep+"User name mapping: "+str(mapping)+sep+"Anonymized files: "+file_name)
##############################################

##############################################
#####           Main program             #####
##############################################

def main():

    # Arguments parsing
    parser=argparse.ArgumentParser()
    parser.add_argument("log_dir1")
    parser.add_argument("log_dir2")
    parser.add_argument("-q", "--question", type=int)

    args = parser.parse_args()
    
    log_name1=args.log_dir1
    log_name2=args.log_dir2
    q_id=args.question

    log_url1="file://"+os.path.join(os.getcwd(),log_name1)
    log_url2="file://"+os.path.join(os.getcwd(),log_name2)

    # Spark initialization
    conf = SparkConf().setAppName("log_analyzer").setMaster("local")
    sc = SparkContext(conf=conf)

    # Load logs
    log1=sc.textFile(log_url1)
    log2=sc.textFile(log_url2)

    all_pairs=[(log_name1,log1),(log_name2,log2)]

    # Run the function corresponding to the requested question,
    # or all the questions if no question was specified
    questions={1: q1,
               2: q2,
               3: q3,
               4: q4,
               5: q5,
               6: q6,
               7: q7,
               8: q8,
               9: q9,
    }
    if q_id is not None:
        questions[q_id](all_pairs)
    else:
        for q in questions.keys():
            questions[q](all_pairs)
        
if __name__=='__main__':
    main()
