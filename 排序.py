#!/usr/bin/env python
# coding: utf-8

# In[25]:


import openpyxl
import xlrd
import re
from itertools import groupby
from numpy  import * 
import numpy as np

xl = open("testresult.txt")
line = xl.readline()
line = line.rstrip("\n")
rslt = line.split(":") 
result3=[]
f = open("ranked.txt", 'w')

while line:             
    rslt= line.split("\t") 
    Num  =len(rslt)
    rslt2=[]
    for i in range(1,Num):
        rslt2.append(rslt[i].split(":"))
    line = xl.readline()  
    line = line.rstrip("\n")
    rslt2.sort(key=lambda rslt2: rslt2[0])
    rslt.append(rslt2)
    f.write(rslt[0])
    f.write("\n")
    for var in rslt2:
        f.writelines(var)
        f.write("\n")
    f.close()
    print(rslt)


# In[ ]:




