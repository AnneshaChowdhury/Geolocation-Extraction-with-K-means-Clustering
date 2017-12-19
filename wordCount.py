predictedRDD.coalesce(1).saveAsTextFile("result.txt")

text_file = sc.textFile("result.txt")
counts = text_file.flatMap(lambda line: line.split(" ")) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)
counts.saveAsTextFile("count.txt")
