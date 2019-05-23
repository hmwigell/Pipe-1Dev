#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri May 17 12:33:17 2019
This is the 1st part of the pipeline to merge the order 
Header and detail file.
-checks to see if a product has been sold 3 times
-outputs: ordfile
-outputs no_pred_ordfile

@author: mac
"""


import csv
import os
import pandas as pd
from datetime import datetime


# ******* Setting your environment *********
# change your directory paths
# directory where the main input file is stored
my_path = "/Users/mac/Desktop"
# working directory for writing temp files
my_wrkdir = "/Users/mac/Desktop/"
# change your input files
input_file = "order_detail.csv"
key_file = "order_header.csv"
out_file_orders = "orderdatepairs.csv"
out_file_no_prediction = "no_prd_list.csv"
# main input file contains the detail info
csv_read_path = os.path.join(my_path, input_file)
""" 
#Opens the main input file of expected structure
#row_id=0 , cust_id =1, order_id = 2  prod_id = 3 case_qty = 4 order_date = 5,sell_day =6

"""



orddet = pd.read_csv(csv_read_path,sep=',', header=0,
dtype={"cliepdnt_orderid":int,"client_custid":int,"client_prodid":int,
"cases_ordered":int,"units_ordered":int,"cases_inven":int,"units_inven":int,
"discount":float,"discount_flag":int})

orddet['row_id'] = orddet.index       

print(orddet.head())
print(orddet.shape)

csv_read_path = os.path.join(my_path, key_file)

ordhead = pd.read_csv(csv_read_path)
ordhead = ordhead.astype({"client_custid":int, "client_orderid":int})
ordhead["order_date"] = pd.to_datetime (ordhead["order_date"])
ordhead["delivery_date"] = pd.to_datetime (ordhead["delivery_date"])
print(ordhead.head())

#print(ordhead.dtypes)
# merge the order header and detail records
ordlist1 = pd.merge(ordhead, orddet, on = ["client_orderid", "client_custid"])


#remove duplicate column names
print(ordlist1.head())
ordlist2 = ordlist1.filter(["row_id", "client_orderid", "client_custid","client_prodid", "cases_ordered", "units_ordered","cases_inven","units_inven","discount","discount_flag","order_date","delivery_date"], axis =1)

print(list(ordlist2))
print(ordlist2.shape)

#filter the zero ccase orders ou of the data set
ordlist3 = ordlist2[ordlist2["cases_ordered"] > 0]

print(list(ordlist3))
print(ordlist3.shape)

# group by cid and pid with counts of rows
ordlist4 = ordlist3.groupby(['client_custid', 'client_prodid'])
ordlist5 = ordlist4.size().reset_index(name = 'pc_count')
 #  ordlist4.size().reset_index(name = "rowcount")
print("*****4****")
print(ordlist5.head())
print(ordlist5.shape)

ordlist6 = ordlist5[ordlist5["pc_count"] > 2] 
print(ordlist6.shape)
print(ordlist6.head())

ordlist7 = ordlist2.merge( ordlist5,  on = ["client_custid", "client_prodid"], how = 'left')
print("*******   7 *****")
print(ordlist7.shape)
print(ordlist7.head(20)) 
print(list(ordlist7))
ordlist7["pc_count"].fillna(0, inplace =True)

orddet_not_predicted = ordlist7[ordlist7["pc_count"] <3]
orddet_pipe_1 = ordlist7[ordlist7["pc_count"] >2]

orddet_not_predicted.to_csv('orddet_not_predicted.csv', index =False ,header =True)
orddet_pipe_1.to_csv       ('orddet_pipe_1.csv', index =False , header =True)
