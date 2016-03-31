# IPDispatch
根据用户端IP进行访问调度。就是通常说的302调度。

## 功能：
1. 调度方式：基于一致性哈希的调度，轮询，权重。
2.  通过流量调度，实现节点过载保护。当节点流量达到峰值时，将一部分流量调度给其他节点或第三方CDN。
3. 调度系统的过载保护。(未开发)
4. 提供API，获取或变更配置与状态。
5. 支持多域名配置，每个域名不同的调度策略。支持别名。
6. 支持gracfuldown。支持不中断服务的情况下升级程序（二进制包）.类似于nginx的Upgrading To a New Binary On The Fly.

主配置项为：IPDisp-path。设定配置目录（绝对路径）。
./IPDispatch -c IPDisp-path

## 配置目录格式：
1. $IPDisp-path/ipz：IP地址库。
2. $IPDisp-path/hostname/view.conf：区域+运营商与节点的对应关系，也就是调度策略。
3. $IPDisp-path/hostname/node.conf：调度配置信息。<br>
[conf]<br>
alias=abc.test.com<br>
[node-name]<br>
server=ip,id,weight,status<br>
server=ip1,id1,weight,status<br>
\#weight：必须是百分制，所有server的weight相加等于100。<br>
bw=当前使用带宽（MB）<br>
maxbw=节点带宽（MB）<br>
freebw=剩余带宽（MB）。小于此值时，将会向overflow2node切流量<br>
overflow2node=node-name<br>
status=up|down<br>
balance=h|r|A。h：一致性哈希调度；r:轮训；A：随机数调度。<br>

## 接口：
1. 设置节点或服务器相关设置。<br>
\# 地址：/ipdadmin/set<br>
\# 请求方式：POST<br>
\# 参数：<br>
\#    host：指定需要操作的域名<br>
\#    object：设置需要操作的对象，有两种值：node或server。<br>
\#    value：需要设置的值。对于节点可以设置：bw和status；对于服务器可以设置weight和status。value参数可以有多个。<br>
\# 响应结果：返回状态码为200代表成功，其他为设置失败
