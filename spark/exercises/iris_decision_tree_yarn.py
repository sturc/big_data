import sys
sys.path.append("..")
from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, VectorAssembler, IndexToString
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

input_file = "hdfs:///data/iris.data"

#create a SparkSession
spark = (SparkSession
       .builder
       .appName("Iris data analysis")
       .master("yarn")
       .getOrCreate())

# load data file.
# create a DataFrame using an infered Schema 
df = spark.read.option("header", "false")        .option("inferSchema", "true")        .option("delimiter", ",")        .csv(input_file)        .withColumnRenamed("_c0","sepal length")       .withColumnRenamed("_c1","sepal width")        .withColumnRenamed("_c2","petal length")       .withColumnRenamed("_c3","petal width")        .withColumnRenamed("_c4","class")

labelIndexer = StringIndexer().setInputCol("class").setOutputCol("label").fit(df)
featureCols = ['sepal length', 'sepal width', 'petal length', 'petal width']
assembler =  VectorAssembler(outputCol="features", inputCols=list(featureCols))
labeled_point_ds = assembler.transform(labelIndexer.transform(df))

#split data for testing
splits = labeled_point_ds.randomSplit([0.6, 0.4 ], 5756)
train = splits[0]
test = splits[1]


# Build the Decision Tree Model
dt = DecisionTreeClassifier(labelCol="label", featuresCol="features", impurity="entropy")
dtModel = dt.fit(train)

# Build an Evaluator
evaluator =  MulticlassClassificationEvaluator(labelCol="label",predictionCol="prediction", metricName="accuracy")

predConverter = IndexToString(inputCol="prediction",outputCol="predictedLabel",labels=labelIndexer.labels)

predictions = dtModel.transform(test)
predictionsConverted = predConverter.transform(predictions)

accuracy = evaluator.evaluate(predictions)
print("Test Error = " ,(1.0 - accuracy))

predictionsConverted.select("prediction", "label", "predictedLabel", "class", "features").show()

spark.stop()

