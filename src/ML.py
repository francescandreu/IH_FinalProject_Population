import pandas as pd
import numpy as np

from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
#from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
from sklearn import metrics
import os
from sys import platform


def show_metrics(metrics, y_test, y_pred):
    print("MAE - Mean absolute error", metrics.mean_absolute_error(y_test, y_pred))
    print("MSE - Mean squeared error", metrics.mean_squared_error(y_test, y_pred))
    print("RMSE - Root mean squared error", np.sqrt(metrics.mean_absolute_error(y_test, y_pred)))
    print("R2 - Mean absolute error", metrics.r2_score(y_test, y_pred))


if __name__ == '__main__':
    df_train = pd.read_csv("data\output\df_train.csv")

    df_train = df_train.dropna(axis = 0)
    print("[Dropped nans]")

    y = df_train['Population']
    X = df_train.drop(['Population'], axis=1)
    print("[Df splitted]")

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)
    print("[X-Y built]")

    reg = LinearRegression().fit(X_train, y_train)
    print("[Regressor built and fitted]")

    y_pred = reg.predict(X_test)
    m = metrics
    show_metrics(m, y_test, y_pred)
