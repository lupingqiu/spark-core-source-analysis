# RDD 算子

## 1 RDD的基本转换算法

### 1.1 map

&emsp;&emsp;将一个`RDD`中的每个数据项，通过`map`中的函数映射变为一个新的元素。输入分区与输出分区是一对一的，即：有多少个输入分区，就有多少个输出分区。

```scala
hadoop fs -cat /tmp/lxw1234/1.txt
hello world
hello spark
hello hive
 
//读取HDFS文件到RDD
scala> var data = sc.textFile("/tmp/lxw1234/1.txt")
data: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[1] at textFile at :21
 
//使用map算子
scala> var mapresult = data.map(line => line.split("\\s+"))
mapresult: org.apache.spark.rdd.RDD[Array[String]] = MapPartitionsRDD[2] at map at :23
 
//运算map算子结果
scala> mapresult.collect
res0: Array[Array[String]] = Array(Array(hello, world), Array(hello, spark), Array(hello, hive))
```

### 1.2 flatMap

&emsp;&emsp;第一步和`map`一样，但是最后会将所有的输出分区合并成一个。

```scala
//使用flatMap算子
scala> var flatmapresult = data.flatMap(line => line.split("\\s+"))
flatmapresult: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[3] at flatMap at :23
 
//运算flagMap算子结果
scala> flatmapresult.collect
res1: Array[String] = Array(hello, world, hello, spark, hello, hive)
```

&emsp;&emsp;使用`flatMap`时候需要注意：`flatMap`会将字符串看成是一个字符数组。

```scala
scala> data.map(_.toUpperCase).collect
res32: Array[String] = Array(HELLO WORLD, HELLO SPARK, HELLO HIVE, HI SPARK)
scala> data.flatMap(_.toUpperCase).collect
res33: Array[Char] = Array(H, E, L, L, O,  , W, O, R, L, D, H, E, L, L, O,  , S, P, A, R, K, H, E, L, L, O,  , H, I, V, E, H, I,  , S, P, A, R, K)
```

&emsp;&emsp;再看下面的例子，和预期的结果一致。这是因为这次`map`函数中返回的类型为`Array[String]`，并不是`String`。`flatMap`只会将`String`扁平化成字符数组，并不会把`Array[String]`也扁平化成字符数组。

```scala
scala> data.map(x => x.split("\\s+")).collect
res34: Array[Array[String]] = Array(Array(hello, world), Array(hello, spark), Array(hello, hive), Array(hi, spark))

scala> data.flatMap(x => x.split("\\s+")).collect
res35: Array[String] = Array(hello, world, hello, spark, hello, hive, hi, spark)
```

### 1.3 distinct

&emsp;&emsp;对RDD中的元素进行去重操作。

```scala
scala> data.flatMap(line => line.split("\\s+")).collect
res61: Array[String] = Array(hello, world, hello, spark, hello, hive, hi, spark)

scala> data.flatMap(line => line.split("\\s+")).distinct.collect
res62: Array[String] = Array(hive, hello, world, spark, hi)
```

### 1.4 coalesce

&emsp;&emsp;该函数的定义如下：

```scala
def coalesce(numPartitions: Int, shuffle: Boolean = false,
               partitionCoalescer: Option[PartitionCoalescer] = Option.empty)
              (implicit ord: Ordering[T] = null)
```

&emsp;&emsp;该函数用于将`RDD`进行重分区，使用`HashPartitioner`。第一个参数为重分区的数目，第二个为是否进行`shuffle`，默认为`false`，当默认为`false`时，重分区的数目不能大于原有数目;
第三个参数表示如何重分区的实现，默认为空。

```scala
scala> var data = sc.textFile("/tmp/lxw1234/1.txt")
data: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[53] at textFile at :21
 
scala> data.collect
res37: Array[String] = Array(hello world, hello spark, hello hive, hi spark)
 
scala> data.partitions.size
res38: Int = 2  //RDD data默认有两个分区
 
scala> var rdd1 = data.coalesce(1)
rdd1: org.apache.spark.rdd.RDD[String] = CoalescedRDD[2] at coalesce at :23
 
scala> rdd1.partitions.size
res1: Int = 1   //rdd1的分区数为1
 
scala> var rdd1 = data.coalesce(4)
rdd1: org.apache.spark.rdd.RDD[String] = CoalescedRDD[3] at coalesce at :23
 
scala> rdd1.partitions.size
res2: Int = 2   //如果重分区的数目大于原来的分区数，那么必须指定shuffle参数为true
 
scala> var rdd1 = data.coalesce(4,true)
rdd1: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[7] at coalesce at :23
 
scala> rdd1.partitions.size
res3: Int = 4
```

### 1.5 repartition

```scala
def repartition(numPartitions: Int)(implicit ord: Ordering[T] = null): RDD[T] = withScope {
    coalesce(numPartitions, shuffle = true)
  }
```
&emsp;&emsp;该函数就是`coalesce`函数第二个参数为`true`的实现。

```scala
scala> var rdd2 = data.repartition(1)
rdd2: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[11] at repartition at :23

scala> rdd2.partitions.size
res4: Int = 1

scala> var rdd2 = data.repartition(4)
rdd2: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[15] at repartition at :23

scala> rdd2.partitions.size
res5: Int = 4
```
