import findspark
findspark.init() # this allows you to import pyspark as a library
import pyspark
from pyspark.sql import SparkSession as SS # to get your spark session.
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
from pyspark.sql.types import *
from pyspark.sql.functions import isnan, when, count, col
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# Now instantiate your spark session using builder..
spark= SS.builder.master("local").appName("Predcting-Diabetes-Readmission").getOrCreate()

rawData = spark.read\
            .format("csv")\
            .option("header", "true")\
            .load("diabetic_data.csv")


rawData = rawData.drop('encounter_id', 'patient_nbr')


field_names = rawData.columns
fields = [StructField(field_name, StringType(), True) for field_name in field_names]
rddWithoutQues = rawData.rdd.map(lambda x: [None if string == "?" else string for string in x])
schema = StructType(fields)
diabetes = spark.createDataFrame(rddWithoutQues, schema)




def modify_values(r):
    if r == ">30" or r =="<30":
        return "Yes"
    else:
        return "No"
ol_val = udf(modify_values, StringType())
diabetes = diabetes.withColumn("readmitted",ol_val(diabetes.readmitted))


# Print out missing values per column
#diabetes.select([count(when(col(c).isNull(),c)).alias(c) for c in diabetes.columns]).show()

diabetes = diabetes.drop('weight', 'payer_code', 'medical_specialty')
diabetes = diabetes.withColumn("diag_2", diabetes["diag_2"].cast(DoubleType()))
diabetes = diabetes.withColumn("diag_3", diabetes["diag_3"].cast(DoubleType()))
diabetes = diabetes.withColumn("diag_1", diabetes["diag_1"].cast(DoubleType()))
diabetes = diabetes.withColumn("time_in_hospital", diabetes["time_in_hospital"].cast(DoubleType()))
diabetes = diabetes.withColumn("num_lab_procedures", diabetes["num_lab_procedures"].cast(DoubleType()))
diabetes = diabetes.withColumn("num_medications", diabetes["num_medications"].cast(DoubleType()))
diabetes = diabetes.withColumn("number_emergency", diabetes["number_emergency"].cast(DoubleType()))
diabetes = diabetes.withColumn("number_inpatient", diabetes["number_inpatient"].cast(DoubleType()))
diabetes = diabetes.withColumn("number_diagnoses", diabetes["number_diagnoses"].cast(DoubleType()))

# Dropping the remaining few NA rows
diabetes = diabetes.dropna()

# Redundant and unbalacned feature list imported from R
diabetes = diabetes.drop('examide', 'citoglipton', 'metformin-rosiglitazone', 'metformin-pioglitazone', 
         'glimepiride-pioglitazone', 'citoglipton, examide', 'acetohexamide',
    'repaglinide', 'nateglinide', 'chlorpropamide', 'tolbutamide', 'acarbose', 'miglitol',
         'troglitazone', 'tolazamide', 'glyburide-metformin', 'glipizide-metformin')

		

# Creating a list of STRING dtype columns
cols_to_index_1 = [x[0] if x[1] == "string" else None for x in diabetes.dtypes]
cols_to_index = [x for x in cols_to_index_1 if x != None]

# Creating the indexers for each column
indexers = [StringIndexer(inputCol=column, outputCol=column+"_index") for column in cols_to_index]
indexers_fitted = [ind.fit(diabetes) for ind in indexers]

# Passing it on to the Pipeline function
# Pipeline transforms our DF using "stages" 
pipeline = Pipeline(stages=indexers_fitted)
diabetes_indexed = pipeline.fit(diabetes).transform(diabetes)

diabetes_indexed = diabetes_indexed.drop('race',
 'gender',
 'age',
 'admission_type_id',
 'discharge_disposition_id',
 'admission_source_id',
 'num_procedures',
 'number_outpatient',
 'max_glu_serum',
 'A1Cresult',
 'metformin',
 'glimepiride',
 'glipizide',
 'glyburide',
 'pioglitazone',
 'rosiglitazone',
 'insulin',
 'change',
 'diabetesMed',
 'readmitted')

diabetes_indexed = diabetes_indexed.withColumnRenamed("readmitted_index", "readmitted")

cols_to_ohe = cols_to_index
cols_to_ohe.remove("readmitted")
cols_to_ohe_ = [col+"_index" for col in cols_to_ohe]


output_ohe_cols = [x+"_vector" for x in cols_to_ohe_]

from pyspark.ml.feature import OneHotEncoderEstimator
encoder = OneHotEncoderEstimator(inputCols=cols_to_ohe_,
                                 outputCols=output_ohe_cols)

model_ohe = encoder.fit(diabetes_indexed)
diabetes_ohe = model_ohe.transform(diabetes_indexed)

diabetes_ohe = diabetes_ohe.drop('race_index',
 'gender_index',
 'age_index',
 'admission_type_id_index',
 'discharge_disposition_id_index',
 'admission_source_id_index',
 'num_procedures_index',
 'number_outpatient_index',
 'max_glu_serum_index',
 'A1Cresult_index',
 'metformin_index',
 'glimepiride_index',
 'glipizide_index',
 'glyburide_index',
 'pioglitazone_index',
 'rosiglitazone_index',
 'insulin_index',
 'change_index',
 'diabetesMed_index')

 
# The following code will take in all of these columns and convert it to 1 column named "features" which will store data of
# ALL features for 1 record (row)

assembler = VectorAssembler(
    inputCols=['time_in_hospital',
 'num_lab_procedures',
 'num_medications',
 'number_emergency',
 'number_inpatient',
 'diag_1',
 'diag_2',
 'diag_3',
 'number_diagnoses',
 'metformin_index_vector',
 'admission_source_id_index_vector',
 'rosiglitazone_index_vector',
 'glimepiride_index_vector',
 'discharge_disposition_id_index_vector',
 'glipizide_index_vector',
 'max_glu_serum_index_vector',
 'gender_index_vector',
 'number_outpatient_index_vector',
 'race_index_vector',
 'diabetesMed_index_vector',
 'admission_type_id_index_vector',
 'A1Cresult_index_vector',
 'change_index_vector',
 'glyburide_index_vector',
 'age_index_vector',
 'insulin_index_vector',
 'pioglitazone_index_vector',
 'num_procedures_index_vector'],
    outputCol="features")

output = assembler.transform(diabetes_ohe)

output = output.drop('time_in_hospital',
 'num_lab_procedures',
 'num_medications',
 'number_emergency',
 'number_inpatient',
 'diag_1',
 'diag_2',
 'diag_3',
 'number_diagnoses',
 'metformin_index_vector',
 'admission_source_id_index_vector',
 'rosiglitazone_index_vector',
 'glimepiride_index_vector',
 'discharge_disposition_id_index_vector',
 'glipizide_index_vector',
 'max_glu_serum_index_vector',
 'gender_index_vector',
 'number_outpatient_index_vector',
 'race_index_vector',
 'diabetesMed_index_vector',
 'admission_type_id_index_vector',
 'A1Cresult_index_vector',
 'change_index_vector',
 'glyburide_index_vector',
 'age_index_vector',
 'insulin_index_vector',
 'pioglitazone_index_vector',
 'num_procedures_index_vector')




training, testing = output.randomSplit([0.8,0.2])
rf = RandomForestClassifier(numTrees=100, maxDepth=6, labelCol="readmitted", seed=42,
                           featureSubsetStrategy='onethird')
model = rf.fit(training)

predictions = model.transform(testing)

# Select (prediction, true label) and compute test error
evaluator = MulticlassClassificationEvaluator(
    labelCol="readmitted", predictionCol="prediction", metricName="accuracy")

accuracy = evaluator.evaluate(predictions)
print("Accuracy for Random Forest = %g" % accuracy)


lr = LogisticRegression(maxIter=100, regParam=0.3, elasticNetParam=0.8, labelCol="readmitted", 
                        featuresCol="features")
lrModel = lr.fit(training)

preds_lr = lrModel.transform(testing)

# Select (prediction, true label) and compute test error
evaluator = MulticlassClassificationEvaluator(
    labelCol="readmitted", predictionCol="prediction", metricName="accuracy")

accuracy = evaluator.evaluate(preds_lr)
print("Accuracy for Logistic Regression = %g" % accuracy)