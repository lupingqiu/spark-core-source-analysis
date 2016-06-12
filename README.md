# spark 核心源码解析

&emsp;&emsp;本专题对`spark`核心源码进行解析，旨在加深对`spark`的理解。

# 本系列文章支持的spark版本
  
spark 2.x

# 本系列的目录结构

* 1 RDD算子介绍
   * [1.1 RDD基本转换](RDD/RDD-basic-transformations.md)
   * [1.2 RDD键值转换](RDD/RDD-kv-transformations.md)
   * [1.3 RDD action操作](RDD/RDD-actions.md)
* 2 RDD实现分析
* 3 RPC模块分析
* 4 Network模块分析
* 5 Scheduler模块分析
* 6 Deploy模块分析
* 7 Executor模块分析
* 8 Shuffle模块分析
* 9 Storage模块分析
    
# 说明

&emsp;&emsp;本专题的大部分内容来自`spark`源码、`spark`官方文档，并不用于商业用途。转载请注明本专题地址。本专题引用他人的内容均列出了参考文献，如有侵权，请务必邮件通知作者。邮箱地址：endymecy@sina.cn。
&emsp;&emsp;本人水平有限，分析中难免有错误和误解的地方，请大家不吝指教，万分感激。有问题可开`wiki`，一起讨论。


