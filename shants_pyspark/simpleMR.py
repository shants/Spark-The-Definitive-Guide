import __future__ 
import collections
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("someApp")
sc = SparkContext(conf = conf)

def red(a,b):
    n = len(a)
    ans = [0]*n
    for i in range(n):
        ans[i]=a[i]+b[i]

    return ans

def rowWise(row):
    n = row.split(",")
    c = [0]*len(n)
    for i in range(len(n)):
        if n[i]=='1':
            c[i]+=1
    return c


x = sc.parallelize(["1,1,,0,,,0,1,1,1","1,1,,0,1,1,0,1,1,1","1,1,,0,1,1,0,1,1,1"])

c = x.map(rowWise)
y = c.reduce(red)
print(y)
