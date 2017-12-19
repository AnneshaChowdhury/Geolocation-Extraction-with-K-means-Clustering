d = {1: [[3, 6],[0, 1],[5, 1]], 2: [[3, 6]], 3: [[3, 0],[2, 8]]}  # example python dictionary
a = sc.parallelize(d) # generate rdd of keys

def func(key):
  l = []  # empty list
  for i in range(len(d.get(key))):
    l.append({key: d.get(key)[i]})
  return l
  
b = a.flatMap(func)

# b.take(10) gives [{1: [3,6]}, {1: [0,1]}, {1: [5,1]}, {2: [3,6]}, {3: [3,0]}, {3: [2,8]}]
