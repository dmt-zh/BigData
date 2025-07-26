#!.venv/bin/python3.12

import mlflow
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from pandas.core.frame import DataFrame
from sklearn import datasets
from sklearn.linear_model import ElasticNet
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split

mlflow.set_tracking_uri('http://89.169.179.173:5050/')
mlflow.set_experiment('Dmitry Zhigalo 2025-07-26')

#################################################################################################

def eval_metrics(actual: DataFrame, pred: np.ndarray) -> tuple[float]:
    rmse = np.sqrt(mean_squared_error(actual, pred))
    mae = mean_absolute_error(actual, pred)
    r2 = r2_score(actual, pred)
    return rmse, mae, r2

#################################################################################################

def run_experiment(run_name: str, alpha: float, l1_ratio: float, data: DataFrame) -> None:

    with mlflow.start_run(run_name=run_name):
        train, test = train_test_split(data)
        train_x = train.drop(["progression"], axis=1)
        test_x = test.drop(["progression"], axis=1)
        train_y = train[["progression"]]
        test_y = test[["progression"]]

        model = ElasticNet(alpha=alpha, l1_ratio=l1_ratio, random_state=42)
        model.fit(train_x, train_y)
        predicted_qualities = model.predict(test_x)
        rmse, mae, r2 = eval_metrics(test_y, predicted_qualities)

        mlflow.log_param("alpha", alpha)
        mlflow.log_param("l1_ratio", l1_ratio)
        mlflow.log_metric("rmse", rmse)
        mlflow.log_metric("r2", r2)
        mlflow.log_metric("mae", mae)
        
        mlflow.sklearn.log_model(
            model,
            name='ElasticNet_3_sklearn',
            input_example=test_x
        )
    mlflow.end_run()

#################################################################################################

def main() -> None:
    diabetes = datasets.load_diabetes()
    features = diabetes.data
    labels = np.array([diabetes.target]).transpose()

    data = np.concatenate((features, labels), axis=1)
    cols = diabetes.feature_names + ['progression']
    dataframe = pd.DataFrame(data, columns=cols)

    run_experiment('ElasticNet_3', 0.35, 0.72, dataframe)

#################################################################################################

if __name__ == '__main__':
    load_dotenv()
    main()
