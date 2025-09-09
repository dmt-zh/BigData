#!.venv/bin/python3

import mlflow
from os import environ

from dotenv import load_dotenv
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit
from pyspark.sql import DataFrame, SparkSession
from pyspark.ml.classification import GBTClassifier

##############################################################################################

class DriverClassifier:
    def __init__(self, train_df_path: str, test_df_path: str) -> None:
        self._train_df_path = train_df_path
        self._test_df_path = test_df_path
        self._session = SparkSession.builder.appName('DriverClassifier').getOrCreate()
        self._model = GBTClassifier(labelCol='has_car_accident')
        self._evaluator = MulticlassClassificationEvaluator(
            labelCol='has_car_accident',
            predictionCol='prediction',
            metricName='f1',
        )
        self._eval_metrics = ['f1', 'weightedPrecision', 'weightedRecall', 'accuracy']
        self._features = [
            'age',
            'sex_index',
            'car_class_index',
            'driving_experience',
            'speeding_penalties',
            'parking_penalties',
            'total_car_accident',
        ]
        self._model_path = 'spark-model'

    ##########################################################################################

    def _build_pipeline(self) -> Pipeline:
        sex_indexer = StringIndexer(
            inputCol='sex',
            outputCol='sex_index'
        )
        car_class_indexer = StringIndexer(
            inputCol='car_class',
            outputCol='car_class_index'
        )
        assembler = VectorAssembler(
            inputCols=self._features,
            outputCol='features'
        )
        return Pipeline(stages=[sex_indexer, car_class_indexer, assembler, self._model])

    ##########################################################################################

    def _optimize_model(self, pipeline: Pipeline, train_df: DataFrame) -> PipelineModel:
        param_grid = ParamGridBuilder() \
            .addGrid(self._model.maxDepth, [3, 5]) \
            .addGrid(self._model.maxIter, [20, 30]) \
            .addGrid(self._model.maxBins, [16, 32]) \
            .build()

        tvs = TrainValidationSplit(
            estimator=pipeline,
            estimatorParamMaps=param_grid,
            evaluator=self._evaluator,
            trainRatio=0.8,
        )
        models = tvs.fit(train_df)
        return models.bestModel

    ##########################################################################################

    def _log_stages(self, classifier: PipelineModel, train_df: DataFrame) -> None:
        mlflow.log_params(
            {
                'features': self._features,
                'input_columns': train_df.columns,
                'maxDepth': classifier.stages[-1].getMaxDepth(),
                'maxIter': classifier.stages[-1].getMaxIter(),
                'maxBins': classifier.stages[-1].getMaxBins(),
                'target': self._model.getLabelCol(),
            }
        )
        for idx, stage in enumerate(classifier.stages):
            mlflow.log_param(f'stage_{idx}', stage.__class__.__name__)

    ##########################################################################################

    def _evaluate_model(self, predictions: DataFrame) -> None:
        for metric in self._eval_metrics:
            self._evaluator.setMetricName(metric)
            score = self._evaluator.evaluate(predictions)
            mlflow.log_metric(metric, score)

    ##########################################################################################

    def run(self) -> None:
        train_df = self._session.read.parquet(self._train_df_path)
        test_df = self._session.read.parquet(self._test_df_path)
        pipeline = self._build_pipeline()
        model = self._optimize_model(pipeline, train_df)
        predictions = model.transform(test_df)
        self._evaluate_model(predictions)
        model.write().overwrite().save(self._model_path)
        mlflow.spark.log_model(
            model,
            artifact_path=self._model_path,
            registered_model_name=self._model_path,
        )
        self._log_stages(model, train_df)

##############################################################################################

def main() -> None:
    load_dotenv()
    mlflow.set_tracking_uri(environ.get('MLFLOW_TRACKING_URI'))
    mlflow.set_experiment(experiment_name='Driver classification')
    mlflow.start_run(run_name='Classifier')
    classifier = DriverClassifier(
        train_df_path='train.parquet',
        test_df_path='test.parquet',
    )
    classifier.run()
    mlflow.end_run()

##############################################################################################

if __name__ == '__main__':
    main()
