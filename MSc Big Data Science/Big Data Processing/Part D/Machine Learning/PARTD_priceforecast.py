from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.ml.regression import LinearRegression
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler

path = "hdfs://andromeda.student.eecs.qmul.ac.uk/user/ajv31/ethereum_price.csv"
sc = SparkContext()
sql_context = SQLContext(sc)
df_price_forecast = sql_context.read.format('csv').options(header='true', inferschema='true').load(path)
assembler = VectorAssembler(inputCols=['SplyCur','TxTfrCnt'],outputCol='features')
output_price_forecast = assembler.transform(df_price_forecast)
final_data_price_forecast = output_price_forecast.select('features', 'PriceUSD').filter(df_price_forecast['PriceUSD'].isNotNull())
train_data,test_data = final_data_price_forecast.randomSplit([0.7,0.3])
lr = LinearRegression(labelCol='PriceUSD', maxIter=50,regParam=0.3, elasticNetParam=0.5)
model = lr.fit(train_data)
test_results = model.evaluate(test_data)
print(test_results.rootMeanSquaredError)
print(test_results.r2)
un_data = test_data.select('features')
predictions = model.transform(un_data)
predictions.show()
