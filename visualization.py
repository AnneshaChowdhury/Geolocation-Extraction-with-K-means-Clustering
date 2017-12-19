import gmplot


lat = []
lng = []

gmap = gmplot.GoogleMapPlotter(0, 0, 2, apikey=' AIzaSyDLddgAEB0qY8PLEHr-DF-YXPqoK3HdF7E ')

with open('clusters.txt') as inFile:
    for line in inFile.readlines():
        words = line.split(":*:")
        if len(words)==3:
            lat.append(float(words[0]))
            lng.append(float(words[1]))
            keywords=words[2]
            gmap.marker(float(words[0]), float(words[1]), title=keywords)
            
gmap = gmplot.GoogleMapPlotter(0, 0, 2, apikey=' AIzaSyDLddgAEB0qY8PLEHr-DF-YXPqoK3HdF7E ')
gmap.scatter(lat, lng, 'cornflowerblue', edge_width=10,marker=True)
gmap.draw('top10.html')


