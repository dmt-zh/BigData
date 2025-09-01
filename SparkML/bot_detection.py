from argparse import ArgumentParser
from pyspark.ml import Pipeline
from pyspark.ml import PipelineModel
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql import SparkSession

MODEL_PATH = 'spark_ml_model'

##############################################################################################

class BotsDetectionPipeline:
    def __init__(
        self,
        model_path: str,
        train_file_path: str,
        test_file_path: str,
        predictions_file_path: str,
    ) -> None:
        self._model_path = model_path
        self._train_file_path = train_file_path
        self._test_file_path = test_file_path
        self._predictions_file_path = predictions_file_path
        self._session = SparkSession.builder.appName('BotsDetectionPipeline').getOrCreate()

    ##########################################################################################

    def train(self) -> None:
        indexer_user_type = StringIndexer(inputCol='user_type', outputCol='user_type_index')
        indexer_platform = StringIndexer(inputCol='platform', outputCol='platform_index')
        feature = VectorAssembler(
            inputCols=['duration', 'item_info_events', 'select_item_events', 'events_per_min', 'user_type_index', 'platform_index'],
            outputCol='features',
        )
        classifier = RandomForestClassifier(labelCol='is_bot', featuresCol='features')
        pipeline = Pipeline(stages=[indexer_user_type, indexer_platform, feature, classifier])
        train_df = self._session.read.parquet(self._train_file_path)
        p_model = pipeline.fit(train_df)
        p_model.write().overwrite().save(self._model_path)

    ##########################################################################################

    def eval(self) -> None:
        p_model = PipelineModel.load(self._model_path)
        prediction = p_model.transform(valid_df)
        prediction.select(['session_id', 'prediction']).write.save(self._predictions_file_path)

##############################################################################################

def main() -> None:
    parser = ArgumentParser()
    parser.add_argument(
        '--model_path',
        type=str,
        default=MODEL_PATH,
        help='Please set model path.',
    )
    parser.add_argument(
        '--data_path',
        type=str,
        default='session-stat.parquet',
        help='Please set datasets path.'
    )
    parser.add_argument(
        '--test_path',
        type=str,
        default='test.parquet',
        help='Please set datasets path.',
    )
    parser.add_argument(
        '--result_path',
        type=str,
        default='result',
        help='Please set result path.',
    )

    parsed_arguments = parser.parse_args()
    data_path = parsed_arguments.data_path
    test_path = parsed_arguments.test_path
    model_path = parsed_arguments.model_path
    result_path = parsed_arguments.result_path

    bots_pipeline = BotsDetectionPipeline(
        model_path=model_path,
        train_file_path=data_path,
        test_file_path=test_path,
        predictions_file_path=result_path,
    )
    bots_pipeline.train()
    bots_pipeline.eval()

##############################################################################################

if __name__ == '__main__':
    main()
