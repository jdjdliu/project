import numpy as np

# https://blog.csdn.net/SweetSeven_/article/details/95466195

#行李数n，不超过的重量W，重量列表w和价值列表p
# weights=[1,2,5,6,7,9]
# price=[1,6,18,22,28,36]
# n = len(weights) W=13 w=weights p=价值
def fun(n,W,w,p):
    # n=6 W=13
	a=np.array([[0]*(W+1)]*(n+1))
	#依次计算前i个行李的最大价值，n+1在n的基础上进行
	print(a)
	for i in range(1,n+1):
		for j in range(1,W+1):
			print(a)
			if w[i-1]>j:
				a[i,j]=a[i-1,j]
			else:
				a[i,j]=max(a[i-1,j],p[i-1]+a[i-1,j-w[i-1]])#2种情况取最大值
	#print(a)
	print('max value is'+str(a[n,W]))
	findDetail(p,n,a[n,W])
#找到价值列表中的一个子集，使得其和等于前面求出的最大价值，即为选择方案
def findDetail(p,n,v):  
	a=np.array([[True]*(v+1)]*(n+1))
	for i in range(0,n+1):
		a[i][0]=True
	for i in range(1,v+1):
		a[0][i]=False
	for i in range(1,n+1):
		for j in range(1,v+1):
			if p[i-1]>j:
				a[i,j]=a[i-1,j]
			else:
				a[i,j]=a[i-1,j] or a[i-1,j-p[i-1]]
	if a[n,v]:
		i=n
		result=[]
		while i>=0:
			if a[i,v] and not a[i-1,v]:
				result.append(p[i-1])
				v-=p[i-1]
			if v==0:
				break
			i-=1
		print(result)
	else:
		print('error')
weights=[1,2,5,6,7,9]
price=[1,6,18,22,28,36]
#fun(len(weights),13,weights,price)
fun(len(weights),13,weights,price)