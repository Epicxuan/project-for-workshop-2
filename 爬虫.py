#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import requests
import sys
import pandas as pd
from bs4 import BeautifulSoup
import numpy as np
import re,string
import re,string

punc = '~`!#$%^&*()_+-=|\';":/.,?><~·！@#￥%……&*（）——+-=“：’；、。，？》《{}'

for i in range(0,1000):
     #page index
    url=requests.get('http://www.gutenberg.org/ebooks/search/?sort_order=random')
    print(url)
    html = url.text.encode(url.encoding).decode('utf-8')
    bs = BeautifulSoup(html)
    bookname=[]
    #get book name
    namelist = bs.find_all("span",attrs={"class":"title"})
    for tr in namelist:
        info= list(tr.stripped_strings) 
        bookname.append(info[0]) 
    urllist = bs.find_all("a",attrs={"class":"link"})
    booknum = len(urllist)
    for j in range(3,booknum):
         # enter each book website
            
        name=re.sub(r"[%s]+" %punc, "",bookname[j]) #delete punction
        name=name+'.txt' # bookname
        print(name)
        bookurl=requests.get('http://www.gutenberg.org'+urllist[j].get('href')+'.txt.utf-8')
        
        html = bookurl.text.encode(bookurl.encoding).decode('utf-8')
        bs = BeautifulSoup(html,'lxml')
        article=bs.get_text()
        
        # creat a new tet file
        try:
            f = open(name,'a',encoding='utf-8')  
            f.write(article) 
            f.close()
        except: print("error")
    print("page :" +str(i)+"done")
print("done")


# In[ ]:





# In[ ]:




