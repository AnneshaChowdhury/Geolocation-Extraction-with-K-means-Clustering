import json
import sys
import unicodedata
from collections import defaultdict

import gmplot
import nltk
import requests
from newspaper import Article
from pyspark import SparkContext, SparkConf
from pyspark.ml.clustering import KMeans
from pyspark.ml.linalg import Vectors
from pyspark.sql import SparkSession

nltk.download('stopwords')
nltk.download('averaged_perceptron_tagger')

# setting up Spark Context
conf = SparkConf().setMaster("local").setAppName("project")
sc = SparkContext(conf=conf)
spark = SparkSession \
    .builder \
    .getOrCreate()

# using Google News API for collecting articles
url = ('https://newsapi.org/v2/everything?'
       'q=murder&'
       'from=2017-11-19&'
       'sortBy=popularity&'
       'apiKey=a562d2fdb25846999ae74f07e83d467d'
       )

response = requests.get(url)

data = response.json()

# print(data)

# json to python dict 
x1 = json.dumps(data)
jsontoPy = json.loads(x1)

# extract the url from the articles
dictUrl = {}
articleId = 1
for i in jsontoPy['articles']:
    articleUrl = i.get('url')
    dictUrl[articleId] = articleUrl
    articleId += 1

# print(dictUrl[1])
# print(articleUrl)

dictXY = defaultdict(list)
dictCoor = defaultdict(list)
tbl = dict.fromkeys(i for i in range(sys.maxunicode)
                    if unicodedata.category(chr(i)).startswith('P'))
text = {}
id = 0
for i in dictUrl.keys():
    # print(dictUrl[i]+" ",i)
    article = Article(dictUrl[i])
    article.download()
    article.parse()
    articleText = article.text
    if len(articleText) < 10000:
        is_noun = lambda pos: pos[:2] == 'NN'
        tokenized = nltk.word_tokenize(articleText)
        nouns = " ".join([word for (word, pos) in nltk.pos_tag(tokenized) if is_noun(pos)])
        text[id] = nouns.translate(tbl)  # save article text

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
            dictXY[id].append(geoCoord)
            coorString = str(geoCoord[0]) + ',' + str(geoCoord[1])
            dictCoor[coorString].append(id)
        id += 1


articles = sc.parallelize(text)  # create rdd
words = articles.map(lambda x: {x: text[x].split(" ")})


def func(key, d):
    l = []  # empty list
    for i in range(len(d.get(key))):
        l.append({key: d.get(key)[i]})
    return l


a = sc.parallelize(dictXY)  # generate rdd of keys
b = a.flatMap(lambda x: func(x, dictXY))
c = b.collect()

mat = []  # create matrix for latitude/longitude coordinates
for i in c:
    for j in i.values():
        mat.append(j)


data = [(Vectors.dense(x),) for x in mat]
d = spark.createDataFrame(data, ["features"])
kmeans = KMeans(k=10, seed=1)
model = kmeans.fit(d)
transformed = model.transform(d).select("features", "prediction")
print(transformed.rdd.take(10))
rows = transformed.rdd
clustersWithArticles = rows.map(lambda x: (x.prediction, dictCoor.get(str(x.features[0]) + ',' + str(x.features[1])))) \
    .reduceByKey(lambda x, y: x + [e for e in y if e not in x])
print(clustersWithArticles.take(10))


def get_article_text(article_ids):
    article_text = ''
    for article_id in article_ids:
        txt = text.get(article_id)
        if (len(txt) > 1):
            article_text = article_text + txt
    return article_text


clustersWithText = clustersWithArticles.map(lambda x: (x[0], get_article_text(x[1])))


def get_top_ten_words(lines):
    line_words = lines.split(" ")
    words_rdd = sc.parallelize(line_words)
    counts = words_rdd.flatMap(lambda line: line.split(" ")) \
        .filter(lambda x: len(x) > 2) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .sortBy(lambda a: a[1], ascending=False)
    return counts.map(lambda a: a[0]).take(10)


clusterText = clustersWithText.map(lambda x: x[1]).collect()
top10Words = []
for r in clusterText:
    top10Words.append(get_top_ten_words(r))
# print(top10Words)
# lat = []
# lng = []

centers = model.clusterCenters()
print("Cluster Centers: ")
for center in centers:
    print(center)

clusterRDD = clustersWithText.map(
    lambda x: (str(centers[x[0]][1]) + ':*:' + str(centers[x[0]][0]) + ':*:' + str(top10Words[x[0]])))
print(clusterRDD.take(1))
clusterRDD.saveAsTextFile("clusters.txt")

lat = []
lng = []
tittles = []
gmap = gmplot.GoogleMapPlotter(0, 0, 2, apikey=' AIzaSyDLddgAEB0qY8PLEHr-DF-YXPqoK3HdF7E ')

with open('clusters.txt/part-00000') as inFile:
    for line in inFile.readlines():
        words = line.split(":*:")
        if len(words) == 3:
            lat.append(float(words[0]))
            lng.append(float(words[1]))
            keywords = words[2].rstrip()
            tittles.append(keywords)
            gmap.marker(float(words[0]), float(words[1]), '#3B0B39', title=keywords)

gmap = gmplot.GoogleMapPlotter(0, 0, 2, apikey=' AIzaSyDLddgAEB0qY8PLEHr-DF-YXPqoK3HdF7E ')
gmap.coloricon = "http://www.googlemapsmarkers.com/v1/%s/"
gmap.scatter(lat, lng, tittles, 'cornflowerblue', edge_width=10, marker=True)
gmap.draw('top10.html')


