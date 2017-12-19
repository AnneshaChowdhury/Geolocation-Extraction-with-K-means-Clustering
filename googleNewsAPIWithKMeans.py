import json
from collections import defaultdict

import requests
from newspaper import Article
from pyspark import SparkContext
from pyspark.ml.clustering import KMeans
from pyspark.ml.linalg import Vectors
from pyspark.sql import SparkSession

# using Google News API for collecting articles
url = ('https://newsapi.org/v2/everything?'
       'q=Apple&'
       'from=2017-11-19&'
       'sortBy=popularity&'
       'apiKey=a562d2fdb25846999ae74f07e83d467d'
       )

response = requests.get(url)

data = response.json()

# print(data)

x1 = json.dumps(data)
jsontoPy = json.loads(x1)

dictUrl = {}
articleId = 1
for i in jsontoPy['articles']:
    articleUrl = i.get('url')
    dictUrl[articleId] = articleUrl
    articleId += 1

# print(dictUrl[1])
# print(articleUrl)

# dictXY = {}
dictXY = defaultdict(list)
for i in dictUrl.keys():
    # print(dictUrl[i]+" ",i)
    article = Article(dictUrl[i])
    article.download()
    article.parse()
    articleText = article.text
    # print(articleText)

    # using Geoparser.io for location extraction from the articles
    url2 = 'https://geoparser.io/api/geoparser'
    headers = {'Authorization': 'apiKey 27103686864861756'}
    data2 = {'inputText': articleText}
    response2 = requests.post(url2, headers=headers, data=data2)
    jsonData2 = json.dumps(response2.json(), indent=2)
    # print(jsonData2)
    jsontoPy2 = json.loads(jsonData2)

    for j in jsontoPy2['features']:
        geoCoord = j.get('geometry').get('coordinates')
        # print(geoCoord)
        dictXY[i].append(geoCoord)

# print(dictXY)
sc = SparkContext()
sc.setLogLevel("WARN")
spark = SparkSession \
    .builder \
    .getOrCreate()
a = sc.parallelize(dictXY)  # generate rdd of keys


def func(key):
    l = []  # empty list
    for i in range(len(dictXY.get(key))):
        l.append([key, dictXY.get(key)[i]])
    return l


b = a.flatMap(func)

# print(b.take(20))

c = b.map(lambda x: x[1])
# print(c.take(10))

data = [(Vectors.dense(x),) for x in c.collect()]
d = spark.createDataFrame(data, ["features"])
kmeans = KMeans(k=10, seed=1)
model = kmeans.fit(d)
transformed = model.transform(d).select("features", "prediction")
print(transformed.rdd.take(10))
rows = transformed.rdd
predictedRDD = rows.map(lambda x: (x.prediction, [x.features[0], x.features[1]]))
print(predictedRDD.take(10))
centers = model.clusterCenters()
print("Cluster Centers: ")
for center in centers:
    print(center)

# with open("/home/akshay/Documents/gNews.txt", 'w') as outfile:
#     json.dump(location, outfile)
