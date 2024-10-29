from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import StandardScaler

spark = SparkSession.builder.appName("IterativeLinearRegression").getOrCreate()
test_data = spark.read.csv("df_test.csv", header=True, inferSchema=True)
train_data = spark.read.csv("df_train.csv", header=True, inferSchema=True)

feature_columns = ["bedrooms", "grade", "real_bathrooms", "quartile_zone", "month", "living_in_m2"]
assembler = VectorAssembler(inputCols=feature_columns, outputCol="unscaled_features")
test_data = assembler.transform(test_data)
train_data = assembler.transform(train_data)

scaler = StandardScaler(inputCol="unscaled_features", outputCol="features")
scaler_model = scaler.fit(train_data)
train_data = scaler_model.transform(train_data).select("features", "price")
test_data = scaler_model.transform(test_data).select("features", "price")

lr = LinearRegression(featuresCol="features", labelCol="price", maxIter=10, tol=1e-10, elasticNetParam=0.1)
lr_model = lr.fit(train_data)

train_predictions = lr_model.transform(train_data)
train_predictions.select("features", "price", "prediction").show()
evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="price", metricName="mse")
train_mse = evaluator.evaluate(train_predictions)
print(f"Final Training MSE after convergence: {train_mse}")

evaluator_r2 = RegressionEvaluator(predictionCol="prediction", labelCol="price", metricName="r2")
r2_score = evaluator_r2.evaluate(train_predictions)
print(f"Final Training R-squared (R2): {r2_score}")

test_predictions = lr_model.transform(test_data)
test_mse = evaluator.evaluate(test_predictions)
print(f"Final Test MSE after convergence: {test_mse}")

test_r2_score = evaluator_r2.evaluate(test_predictions)
print(f"Final Test R-squared (R2): {test_r2_score}")

print(f"Coefficients: {lr_model.coefficients}")
print(f"Intercept: {lr_model.intercept}")

training_summary = lr_model.summary
print("Objective History (Loss per Iteration):")
for i, loss in enumerate(training_summary.objectiveHistory):
    print(f"Iteration {i+1}: {loss}")

spark.stop()
