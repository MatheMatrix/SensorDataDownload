a = []
for i in range(1, 12 + 1):
	a.append([])

for i in range(0, 12):
	for j in range(0, 12):
		a[i].append(j + 1)
	for k in (13, 25, 37, 49):
		a[i].append(k + i)

b = []
for i in range(len(a)):
	b.extend(a[i])

c = {}
for i in range(1, 192 + 1):
	c[i] = 'ch' + str(b[i - 1])
print a
print b
print c