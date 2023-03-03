# 此例中有多个相同长度的公共子串，但只能获取第一个子串
def find_lcsubstr(s1, s2): 
	# 下面4行不要直接写在循环中，减少计算
	s1_len = len(s1) + 1 					#为方便后续计算，多了1行1列 
	s2_len = len(s2) + 1
	s3_len = len(s1)
	s4_len = len(s2)
	m = [[0 for j in range(s2_len)] for i in range(s1_len)] #生成0矩阵
	maxNum = 0   							#初始最长匹配长度
	p = 0  									#匹配的起始位置
	for i in range(s3_len):
		for j in range(s4_len):
			if s1[i] == s2[j]:				  #相同则累加
				m[i + 1][j + 1] = m[i][j] + 1 #给相同字符赋值，值为左上角值加1
				if m[i + 1][j + 1] > maxNum:
					maxNum = m[i + 1][j + 1]  #获取最大匹配长度
					p = i + 1 				  #记录最大匹配长度的终止位置
	print(m)
	return s1[p - maxNum : p], maxNum   	  #返回最长子串及其长度
print(find_lcsubstr("axxxxsd", "asxxas"))