import nltk
import string
nltk.download('stopwords')
from nltk.corpus import stopwords


with open('result.txt', 'r') as inFile, open('resultNoStop.txt', 'w') as outFile:
    for line in inFile.readlines():
        print(" ".join([word for word in line.lower().translate(str.maketrans('', '', string.punctuation)).split()
                        if word not in stopwords.words('english')]), file=outFile)