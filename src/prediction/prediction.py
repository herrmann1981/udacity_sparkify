"""
Submit a Pyspark application with an input argument on the SPOCK cluster
"""
import os
import logging
import argparse
import datetime
import sys

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as pfunc
from pyspark.sql.functions import when, signum, count, sum, avg, countDistinct
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import DecisionTreeClassifier, RandomForestClassifier,\
    LogisticRegression, MultilayerPerceptronClassifier, LinearSVC
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.mllib.evaluation import MulticlassMetrics

from sklearn.metrics import confusion_matrix


def setup_arguments():
    """
    Set up and parse our arguments
    :return: None
    """
    argparser = argparse.ArgumentParser()
    argparser.add_argument('--startdate',
                           type=str,
                           dest='startdate',
                           help='The date from which on we want to load the orc data.',
                           default='2018-10-01',
                           action='store')
    argparser.add_argument('--enddate',
                           type=str,
                           dest='enddate',
                           help='The date until when we want to load our orc data (End date not included)',
                           default='2018-10-01',
                           action='store')
    argparser.add_argument('--datapath',
                           type=str,
                           dest='datapath',
                           help='The root folder where to find the ORC files',
                           default=os.path.join('..', '..', 'data', 'orc'),
                           action='store')
    args = argparser.parse_known_args()[0]
    return args


def setup_logging():
    """
    Setup our logging so it matches the pyspark default log format
    :return: None
    """
    logging.basicConfig(level=logging.INFO)
    logging.getLogger().handlers[0].setFormatter(
        logging.Formatter(fmt='%(asctime)s %(levelname)s PySpark: %(message)s',
                          datefmt='%y/%m/%d %H:%M:%S')
    )


def read_data(spark: SparkSession, orc_path: str, start_date: datetime.datetime,
              end_date: datetime.datetime) -> DataFrame:
    """
    Read in the required ORC files that fall into the specified time window
    :param spark: The Spark Session we are working with
    :param orc_path: The root orc path to search in
    :param start_date: The start date. It is included in the data
    :param end_date: The end date. It is NOT included in the data
    :return: Dataframe containing all read messages
    """
    df_result: DataFrame = None
    current_date = start_date
    while current_date < end_date:
        date_folder = os.path.join(orc_path, 'date=' + current_date.strftime('%Y-%m-%d'))
        df_temp = spark.read.orc(date_folder)
        if not df_result:
            df_result = df_temp
        else:
            df_result = df_result.union(df_temp)
        current_date = current_date + datetime.timedelta(days=1.0)
    return df_result


def calculate_churn(df_messages: DataFrame) -> DataFrame:
    """
    This method calculates a DataFrame that holds the information about user ids and a flag if the user has churned
    (Canceled subscription)
    :param df_messages: The original dataframe with all api log messages
    :return: A dataframe containing the user_id and the churn flag
    """
    logging.info('Calculate churn')
    df_temp = df_messages\
        .withColumn('submit_downgrade', when(df_messages['page'] == 'Submit Downgrade', 1)
                    .otherwise(0))\
        .withColumn('cancellation_confirmation', when(df_messages['page'] == 'Cancellation Confirmation', 1)
                    .otherwise(0))

    df_churn = df_temp.groupBy('userId') \
        .agg(sum('submit_downgrade').alias('count_downgrade'),
             sum('cancellation_confirmation').alias('count_cancellation')
        )

    df_churn = df_churn.withColumn('churn',
                                   when((df_churn.count_downgrade > 0) | (df_churn.count_cancellation > 0), 1)
                                   .otherwise(0))

    df_churn = df_churn\
        .drop('count_downgrade')\
        .drop('count_cancellation')

    return df_churn


def feature_calculate_page_views(df_messages: DataFrame) -> DataFrame:
    """
    Calculate the number of page view per user
    :param df_messages:
    :return:
    """
    logging.info('Calculate page views')
    df_temp = df_messages.groupBy('userid').pivot('page').count()
    df_temp = df_temp.fillna(0)
    # We need to drop the following columns, because they contain the information about users that are churning.
    # With this information we would perfectly identify all users that have churned. But we know that when he / she
    # has a Submit Downgrade request somewhere in the logs. So we need to remove this columns.
    df_temp = df_temp\
        .drop('Cancellation Confirmation')\
        .drop('Submit Downgrade')\
        .drop('Downgrade')\
        .drop('Cancel')
    return df_temp


def feature_calculate_avg_songs_per_session(df_messages: DataFrame) -> DataFrame:
    """
    Calculate the average number of songs a user has played per session
    :param df_messages:
    :return:
    """
    logging.info('Calculate average songs per session')
    df_songs = df_messages.filter(df_messages.page == 'NextSong')
    df_songs_per_session = df_songs.groupBy('userid', 'sessionid').agg(count('*').alias('count'))
    df_result = df_songs_per_session.groupBy('userid').agg(avg('count').alias('avg_songs_per_session'))
    return df_result


def feature_calculate_avg_session_duration(df_data: DataFrame) -> DataFrame:
    """
    Calculate the average session duration for each user
    :param df_data: The messages in our dataset
    :return: DataFrame with userId and average session duration
    """
    df_temp = df_data.groupBy('userid', 'sessionid').agg(pfunc.min('ts').alias('min_session_ts'),
                                                         pfunc.max('ts').alias('max_session_ts'))

    df_result = df_temp.withColumn('session_duration_days',
                                   (df_temp["max_session_ts"]
                                    - df_temp["min_session_ts"]
                                    ) / 1000 / 60 / 60 / 24)
    df_result = df_result.groupBy('userid')\
        .agg(pfunc.avg('session_duration_days').alias('avg_session_duration_days'))

    df_result = df_result.drop('min_session_timestamp').drop('max_session_timestamp')
    return df_result


def feature_calculate_avg_distinct_artists_per_session(df_data: DataFrame) -> DataFrame:
    """
    This method calculates the average number of different artists listened to per session.
    The assumption is that users that quicly browse through larger number of songs / artists did not find what
    they want to hear.
    :param df_data: The messages in our dataset
    :return: DataFrame containing the userid and the average number of distinct artists per session
    """
    df_temp_songs = df_data.filter(df_data['artist'].isNotNull())
    df_temp = df_temp_songs.groupBy('userid', 'sessionid', 'artist')\
        .agg(pfunc.count('*'))

    df_temp = df_temp.groupBy('userid', 'sessionid')\
        .agg(pfunc.count('artist').alias('num_artists'))

    df_result = df_temp.groupBy('userid')\
        .agg(pfunc.avg('num_artists').alias('avg_num_artists_per_session'))

    return df_result


def feature_calculate_browser_usage(df_data: DataFrame) -> DataFrame:
    """
    This function calculates a DataFrame indicating for each user if he / she uses certain browsers
    :param df_data: The messages in our dataset
    :return: DataFrame containing the userid and columns for each major browser indicating if the user is using them
    """
    df_temp = df_data.groupBy('userid', 'userAgent_browser_family')\
        .agg(pfunc.count('*'))

    df_temp = df_temp\
        .withColumn('uses_chrome', df_data['userAgent_browser_family'].like('%Chrome%'))\
        .withColumn('uses_safari', df_data['userAgent_browser_family'].like('%Safari%'))\
        .withColumn('uses_firefox', df_data['userAgent_browser_family'].like('%Firefox%'))\
        .withColumn('uses_ie', df_data['userAgent_browser_family'].like('%IE%'))

    df_result = df_temp.groupBy('userid')\
        .agg(pfunc.max('uses_chrome').alias('uses_chrome'),
             pfunc.max('uses_safari').alias('uses_safari'),
             pfunc.max('uses_firefox').alias('uses_firefox'),
             pfunc.max('uses_ie').alias('uses_ie'))

    df_result = df_result\
        .withColumn('uses_chrome', df_result['uses_chrome'].cast('integer'))\
        .withColumn('uses_safari', df_result['uses_safari'].cast('integer'))\
        .withColumn('uses_firefox', df_result['uses_firefox'].cast('integer'))\
        .withColumn('uses_ie', df_result['uses_ie'].cast('integer'))

    return df_result


def feature_calculate_os_usage(df_data: DataFrame) -> DataFrame:
    """
    This function calculates a DataFrame indicating for each user if he / she uses certain operation system
    :param df_data: The messages in our dataset
    :return: DataFrame containing the userid and columns for each major OS indicating if the user is using them
    """
    df_temp = df_data.groupBy('userid', 'userAgent_os_family')\
        .agg(pfunc.count('*'))

    df_temp = df_temp\
        .withColumn('uses_windows', df_data['userAgent_os_family'].like('%Windows%'))\
        .withColumn('uses_mac', df_data['userAgent_os_family'].like('%Mac%'))\
        .withColumn('uses_ios', df_data['userAgent_os_family'].like('%ios%'))\
        .withColumn('uses_android', df_data['userAgent_os_family'].like('%Android%'))\
        .withColumn('uses_linux', df_data['userAgent_os_family'].like('%Linux%'))

    df_result = df_temp.groupBy('userid')\
        .agg(pfunc.max('uses_windows').alias('uses_windows'),
             pfunc.max('uses_mac').alias('uses_mac'),
             pfunc.max('uses_ios').alias('uses_ios'),
             pfunc.max('uses_android').alias('uses_android'),
             pfunc.max('uses_linux').alias('uses_linux'))

    df_result = df_result\
        .withColumn('uses_windows', df_result['uses_windows'].cast('integer'))\
        .withColumn('uses_mac', df_result['uses_mac'].cast('integer'))\
        .withColumn('uses_ios', df_result['uses_ios'].cast('integer'))\
        .withColumn('uses_android', df_result['uses_android'].cast('integer'))\
        .withColumn('uses_linux', df_result['uses_linux'].cast('integer'))

    return df_result


def feature_calculate_mobile_usage(df_data: DataFrame) -> DataFrame:
    """
    Method to calculate a column if the user has used mobile version of the service as well as a column
    indicating if he uses the non-mobile version of the service
    :param df_data: The messages we are using in our analysis
    :return: Feature Dataset with userid, uses_mobile, uses_non_mobile
    """
    df_temp = df_data.groupBy('userid', 'userAgent_is_mobile') \
        .agg(pfunc.count('*'))
    df_temp = df_temp\
        .withColumn('uses_mobile', df_temp['userAgent_is_mobile'] == True)\
        .withColumn('uses_non_mobile', df_temp['userAgent_is_mobile'] == False)

    df_result = df_temp.groupBy('userid')\
        .agg(pfunc.max('uses_mobile').alias('uses_mobile'),
             pfunc.max('uses_non_mobile').alias('uses_non_mobile'))

    df_result = df_result\
        .withColumn('uses_mobile', df_result['uses_mobile'].cast('integer'))\
        .withColumn('uses_non_mobile', df_result['uses_non_mobile'].cast('integer'))

    return df_result


def feature_calculate_pc_usage(df_data: DataFrame) -> DataFrame:
    """
    Method to calculate a column if the user has used pc version of the service as well as a column
    indicating if he uses the non-pc version of the service
    :param df_data: The messages we are using in our analysis
    :return: Feature Dataset with userid, uses_pc, uses_non_pc
    """
    df_temp = df_data.groupBy('userid', 'userAgent_is_pc') \
        .agg(pfunc.count('*'))
    df_temp = df_temp\
        .withColumn('uses_pc', df_temp['userAgent_is_pc'] == True)\
        .withColumn('uses_non_pc', df_temp['userAgent_is_pc'] == False)

    df_result = df_temp.groupBy('userid')\
        .agg(pfunc.max('uses_pc').alias('uses_pc'),
             pfunc.max('uses_non_pc').alias('uses_non_pc'))

    df_result = df_result\
        .withColumn('uses_pc', df_result['uses_pc'].cast('integer'))\
        .withColumn('uses_non_pc', df_result['uses_non_pc'].cast('integer'))

    return df_result


def feature_calculate_tablet_usage(df_data: DataFrame) -> DataFrame:
    """
    Method to calculate a column if the user has used tablet version of the service as well as a column
    indicating if he uses the non-tablet version of the service
    :param df_data: The messages we are using in our analysis
    :return: Feature Dataset with userid, uses_tablet, uses_non_tablet
    """
    df_temp = df_data.groupBy('userid', 'userAgent_is_tablet') \
        .agg(pfunc.count('*'))
    df_temp = df_temp\
        .withColumn('uses_tablet', df_temp['userAgent_is_tablet'] == True)\
        .withColumn('uses_non_tablet', df_temp['userAgent_is_tablet'] == False)

    df_result = df_temp.groupBy('userid')\
        .agg(pfunc.max('uses_tablet').alias('uses_tablet'),
             pfunc.max('uses_non_tablet').alias('uses_non_tablet'))

    df_result = df_result\
        .withColumn('uses_tablet', df_result['uses_tablet'].cast('integer'))\
        .withColumn('uses_non_tablet', df_result['uses_non_tablet'].cast('integer'))

    return df_result


def evaluate_classifier(prediction):
    """
    This method is used to evaluate the performance of a classifier
    :param prediction: The prediction dataframe of the classifier
    :return: the accuracy and the confusion matrix for the classifier
    """
    logging.info('Evaluate Classifier')
    accuracy = -1
    cm = [[-1, -1], [-1, -1]]
    try:
        evaluator = MulticlassClassificationEvaluator(predictionCol="prediction", labelCol='churn')
        accuracy = evaluator.evaluate(prediction)

        prediction_and_labels = prediction.select(['prediction', 'churn'])
        prediction_and_labels = prediction_and_labels\
            .withColumn('churn', prediction_and_labels['churn'].cast('float'))
        # prediction_and_labels.show(3)
        metrics = MulticlassMetrics(prediction_and_labels.rdd)
        f1score = metrics.fMeasure(1.0)

        y_pred = prediction.select("prediction").collect()
        y_orig = prediction.select("churn").collect()

        cm = confusion_matrix(y_orig, y_pred)
    except Exception as error:
        logging.warning('Classificator evaluation failed: %s', str(error.args))
    return accuracy, f1score, cm


def log_confusion_matrix(confusionmatrix, classifier_name='Decision Tree'):
    """
    Logs the confusion matrix to the console
    :param confusionmatrix: The confusion matrix we want to log
    :param classifier_name: The name of the classifier we want to show
    :return: None
    """
    logging.info('Confusion Matrix: %s', classifier_name)
    for row in confusionmatrix:
        logline = '|'
        for item in row:
            item_str = str(item)
            logline += item_str + ' ' * (10 - len(item_str))
        logline += '|'
        logging.info(logline)


def main(args: argparse.Namespace):
    """
    Main entry point for the application
    :param args: The input arguments passed to this run
    :return:
    """
    # instantiate and configure the spark session
    spark: SparkSession = SparkSession.builder \
        .appName("sparkify-prediction") \
        .getOrCreate()
    result = 0

    try:
        # set additional configurations
        spark.conf.set('spark.sql.files.maxPartitionBytes', str(128 * 1000 * 1000))
        spark.conf.set('spark.sql.shuffle.partitions', str(10))

        # loading the desired orc data into a dataframe
        logging.info('Loading ORC data')
        start_date = datetime.datetime.strptime(args.startdate, '%Y-%m-%d')
        end_date = datetime.datetime.strptime(args.enddate, '%Y-%m-%d')
        logging.info('Start Date: %s', start_date.strftime('%Y-%m-%d'))
        logging.info('End Date: %s', end_date.strftime('%Y-%m-%d'))
        df_data = read_data(spark, args.datapath, start_date, end_date)

        logging.info('Number of API messages in read dataset: %s', str(df_data.count()))

        # removing messages without userId since we can not use them
        df_data = df_data.filter(df_data['userId'].isNotNull())
        logging.info('Number of API messages where userId is not Null: %s', str(df_data.count()))
        df_distinct_user_ids = df_data.select(countDistinct('userid'))
        logging.info('Number of distinct user ids in data: %s', str(df_distinct_user_ids.collect()[0][0]))

        # Calculate a churn column, meaning a column if the user has submitted a downgrade
        df_churn = calculate_churn(df_data)
        logging.info('In our dataset %s users churned.', str(df_churn.filter(df_churn['churn'] > 0).count()))
        logging.info('In our dataset %s users did not churn.', str(df_churn.filter(df_churn['churn'] == 0).count()))

        # Calculate page views dataframe
        df_page_views = feature_calculate_page_views(df_data)

        # Calculate average number of songs per session
        df_avg_songs_per_session = feature_calculate_avg_songs_per_session(df_data)

        # Calculate average session duration
        df_avg_session_duration = feature_calculate_avg_session_duration(df_data)

        # Calculate average number of distinct artists per session
        df_avg_distinct_artist_per_session = feature_calculate_avg_distinct_artists_per_session(df_data)

        # Calculate browser usage for user
        df_browser_usage = feature_calculate_browser_usage(df_data)

        # Calculate os usage for user
        df_os_usage = feature_calculate_os_usage(df_data)

        # Calculate mobile usage feature
        df_mobile_usage = feature_calculate_mobile_usage(df_data)

        # Calculate pc usage feature
        df_pc_usage = feature_calculate_pc_usage(df_data)

        # Calculate tablat usage feature
        df_tablet_usage = feature_calculate_tablet_usage(df_data)

        # join our individual feature dataframes into one large dataset
        df_features = df_churn\
            .join(df_page_views, ['userid'], 'left')\
            .join(df_avg_songs_per_session, ['userid'], 'left')\
            .join(df_avg_session_duration, ['userid'], 'left')\
            .join(df_avg_distinct_artist_per_session, ['userid'], 'left')\
            .join(df_browser_usage, ['userid'], 'left')\
            .join(df_os_usage, ['userid'], 'left')\
            .join(df_mobile_usage, ['userid'], 'left')\
            .join(df_pc_usage, ['userid'], 'left')\
            .join(df_tablet_usage, ['userid'], 'left')
        df_features = df_features.fillna(0)  # in case there is a no feature value for a certain user id
        df_features = df_features.drop('userid')

        feature_columns = df_features.drop('churn').columns

        # combine our feature into a vector so that the classifier can work with it
        va = VectorAssembler(inputCols=feature_columns, outputCol='features')
        va_df = va.transform(df_features)
        va_df = va_df.select(['features', 'churn'])
        logging.info('Number of rows in our final feature dataframe: %s', str(df_features.count()))

        # now we can split our data into training and test data
        logging.info('Split test and training data')
        (train, test) = va_df.randomSplit([0.8, 0.2])

        # ############# Evaluate different Classifiers #################################################################
        # ------------ Decision Tree Classifier ------------------------------------------------------------------------
        # fit and evaluate Decision Tree Classifier
        logging.info('Train Decision Tree Classifier')
        classifier = DecisionTreeClassifier(featuresCol='features', labelCol='churn')
        classifier = classifier.fit(train)

        logging.info('Test Decision Tree Classifier')
        prediction = classifier.transform(test)
        # prediction.show(3)

        accuracy, f1score, cm = evaluate_classifier(prediction)

        logging.info("Prediction Accuracy (Decision Tree): %s", str(accuracy))
        logging.info("Prediction F1Score (Decision Tree): %s", str(f1score))
        log_confusion_matrix(cm, classifier_name='Decision Tree')

        # ------------ Random Forest Classifier ------------------------------------------------------------------------
        # fit and evaluate Random Forest Classifier
        logging.info('Train Random Forest Classifier')
        classifier = RandomForestClassifier(featuresCol='features', labelCol='churn')
        classifier = classifier.fit(train)

        logging.info('Test Random Forest Classifier')
        prediction = classifier.transform(test)

        accuracy, f1score, cm = evaluate_classifier(prediction)
        logging.info("Prediction Accuracy (Random Forest): %s", str(accuracy))
        logging.info("Prediction F1 (Random Forest): %s", str(f1score))
        log_confusion_matrix(cm, classifier_name='Random Forest')

        # ------------ Logistic Regression Classifier ------------------------------------------------------------------
        # fit and evaluate Logistic Regression Classifier
        logging.info('Train Logistic Regresssion Classifier')
        classifier = LogisticRegression(featuresCol='features', labelCol='churn')
        classifier = classifier.fit(train)

        logging.info('Test Logistic Regression Classifier')
        prediction = classifier.transform(test)

        accuracy, f1score, cm = evaluate_classifier(prediction)
        logging.info("Prediction Accuracy (Logistic Regression): %s", str(accuracy))
        logging.info("Prediction F1Score (Logistic Regression): %s", str(f1score))
        log_confusion_matrix(cm, classifier_name='Logistic Regression')

        # ------------ Multilayer Perceptron Classifier ----------------------------------------------------------------
        # fit and evaluate Multilayer Perceptron Classifier
        # logging.info('Train Multilayer Perceptron Classifier')
        # classifier = MultilayerPerceptronClassifier(featuresCol='features', labelCol='churn') \
        #     .setLayers([4, 5, 4, 3])\
        #     .setBlockSize(128)\
        #     .setSeed(1234)\
        #     .setMaxIter(50)
        # classifier = classifier.fit(train)
        #
        # logging.info('Test Multilayer Perceptron Classifier')
        # prediction = classifier.transform(test)
        #
        # accuracy, f1score, cm = evaluate_classifier(prediction)
        # logging.info("Prediction Accuracy (Multilayer Perceptron): %s", str(accuracy))
        # logging.info("Prediction F1Score (Multilayer Perceptron): %s", str(f1score))
        # log_confusion_matrix(cm, classifier_name='Multilayer Perceptron')

        # ------------ Linear SVC Classifier --------------------------------------------------------------------------
        # fit and evaluate Linear SVC Classifier
        logging.info('Train Linear SVC Classifier')
        classifier = LinearSVC(featuresCol='features', labelCol='churn') \
            .setMaxIter(10)
        classifier = classifier.fit(train)

        logging.info('Test Linear SVC Classifier')
        prediction = classifier.transform(test)

        accuracy, f1score, cm = evaluate_classifier(prediction)
        logging.info("Prediction Accuracy (Linear SVC): %s", str(accuracy))
        logging.info("Prediction F1Score (Linear SVC): %s", str(f1score))
        log_confusion_matrix(cm, classifier_name='Linear SVC')

    except Exception as error:
        logging.fatal('An exception occurred during the analysis: %s', str(error))
        logging.fatal(str(error))
        result = 1

    spark.sparkContext.stop()
    return result


if __name__ == '__main__':
    input_args = setup_arguments()
    setup_logging()
    return_code = main(input_args)
    sys.exit(return_code)
