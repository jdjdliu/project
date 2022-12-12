NOTIFICATION_EMAIL_TEMPLATE = """\
<html>
<head>
<meta charset='UTF-8'>
<style>
body {
  font-family: "Roboto", helvetica, arial, sans-serif;
  font-size: 16px;
  font-weight: 400;
  text-rendering: optimizeLegibility;
}
th {
  color:#D5DDE5;;
  background:#1b1e24;
  border-bottom:4px solid #9ea7af;
  border-right: 1px solid #343a45;
  font-size:23px;
  font-weight: 100;
  padding:24px;
  text-align:left;
  text-shadow: 0 1px 1px rgba(0, 0, 0, 0.1);
  vertical-align:middle;
}
th:first-child {
  border-top-left-radius:3px;
}
th:last-child {
  border-top-right-radius:3px;
  border-right:none;
}
tr {
  border-top: 1px solid #C1C3D1;
  border-bottom-: 1px solid #C1C3D1;
  color:#666B85;
  font-size:16px;
  font-weight:normal;
  text-shadow: 0 1px 1px rgba(256, 256, 256, 0.1);
}
tr:first-child {
  border-top:none;
}
tr:last-child {
  border-bottom:none;
}
tr:nth-child(odd) td {
  background:#EBEBEB;
}
tr:last-child td:first-child {
  border-bottom-left-radius:3px;
}
tr:last-child td:last-child {
  border-bottom-right-radius:3px;
}
td {
  background:#FFFFFF;
  padding:20px;
  text-align:left;
  vertical-align:middle;
  font-weight:300;
  font-size:18px;
  text-shadow: -1px -1px 1px rgba(0, 0, 0, 0.1);
  border-right: 1px solid #C1C3D1;
}
td:last-child {
  border-right: 0px;
}
</style>
</head>
<body>
  <p>数据日期：__DATE__<br/>
     策略名称：__ALGO_NAME__<br/>
     策略描述：__ALGO_DESC__<br/>
  </p>
  <br/>
  __ORDERS_HTML__
  <br/>
  说明：
  <ul>
    <li>数据日期：为策略运行所用的数据日期 （请在接下来的一个交易日开盘时执行交易）</li>
    <li>代码：股票代码</li>
    <li>名称：股票名称</li>
    <li>股数：买入或卖出股票数数量,负数为卖出,正数为买入</li>
    <li>价格：收盘价，用来做买入或卖出价格估算</li>
    <li>资金：分配给此股票的资金量</li>
  </ul>
  补充信息：
  <p>__EXTRA_MESSAGE_HTML__</p>
</body>
</html>
"""
