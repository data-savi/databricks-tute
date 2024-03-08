# Databricks notebook source
dict_1=dict()

dict_1.update({
    "1":"ABC",
    "2":"PQR"
})

dict_1['3']="XYZ"

print(dict_1)
dict_1['4']="XYZ"
for key,val in dict_1.items():
    print(f' key is : {key} and value is :{val}' )