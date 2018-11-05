# -*- coding: utf-8 -*-
"""
Created on Sat Nov  3 12:10:21 2018

@author: Admin
"""

from sklearn.datasets import load_iris
from sklearn.neighbors import KNeighborsClassifier
iris=load_iris()

print(iris.feature_names)
print(iris.target_names)

X=iris.data
y=iris.target

knn=KNeighborsClassifier(n_neighbors=1)


