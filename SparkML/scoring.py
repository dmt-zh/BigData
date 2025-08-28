from pyspark.ml.classification import RandomForestClassifier, RandomForestClassificationModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplitModel, TrainValidationSplit
from pyspark.ml.pipeline import PipelineModel
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import IntegerType
from typing import List, Tuple, Optional

##############################################################################################

class CreditScorer:
    def __init__(self, features: Optional[List[str]] = None) -> None:
        self._session = SparkSession.builder.appName('CreditScorer').getOrCreate()
        if features is not None:
            self._features = features
        else:
            self._features = [
                'age',
                'salary',
                'successfully_credit_completed',
                'credit_completed_amount',
                'active_credits',
                'active_credits_amount',
                'credit_amount',
                'is_marrid',
                'sex_tf',
            ]
        self._sex_feature_tf = StringIndexer(
            inputCol='sex',
            outputCol='sex_tf',
        )
        self._vectorizer = VectorAssembler(
            inputCols=self._features,
            outputCol='features',
        )
        self._evaluator = MulticlassClassificationEvaluator(
            labelCol='is_credit_closed',
            predictionCol='prediction',
            metricName='accuracy'
        )
        self._model = RandomForestClassifier(
            labelCol='is_credit_closed',
            featuresCol='features',
        )

    ##########################################################################################

    def _get_model(self, dataset: DataFrame) -> RandomForestClassificationModel:
        param_grid = ParamGridBuilder() \
            .addGrid(self._model.maxDepth, [2, 3, 4, 5]) \
            .addGrid(self._model.maxBins, [4, 5, 6, 7]) \
            .addGrid(self._model.minInfoGain, [0.001, 0.01, 0.05, 0.1, 0.15, 0.3]) \
            .build()

        tvs = TrainValidationSplit(
            estimator=self._model,
            estimatorParamMaps=param_grid,
            evaluator=self._evaluator,
            trainRatio=0.8,
        )
        model = tvs.fit(dataset)
        return model.bestModel

    ##########################################################################################

    def _prepare_dataset(self, dataset: DataFrame) -> DataFrame:
        dataset = dataset.withColumn('is_marrid', dataset.married.cast(IntegerType()))
        dataset = self._sex_feature_tf.fit(dataset).transform(dataset)
        return self._vectorizer.transform(dataset)

    ##########################################################################################

    def train_and_eval(self) -> Tuple[RandomForestClassificationModel, float]:
        train_df = self._prepare_dataset(self._session.read.parquet('train.parquet'))
        test_df = self._prepare_dataset(self._session.read.parquet('test.parquet'))
        model = self._get_model(train_df)
        predictions = model.transform(test_df)
        accuracy = self._evaluator.evaluate(predictions)
        return model, accuracy

##############################################################################################

def main(*args, **kwargs) -> Tuple[RandomForestClassificationModel, float]:
    scorer = CreditScorer()
    model, accuracy = scorer.train_and_eval()
    return model, accuracy

##############################################################################################

if __name__ == '__main__':
    main()
