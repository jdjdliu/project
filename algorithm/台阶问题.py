class Solution:
    """
    @param n: an integer
    @return: an ineger f(n)
    """
 
    def up(self, n):
        # write your code here
        # if n == 0:
        #     return 0
        L = []
        L.append(1)
        L.append(2)
        for i in range(2, n):
            L.append(L[i - 1] + L[i - 2])
        return L[n - 1]

a = Solution()
print(a.up(10))