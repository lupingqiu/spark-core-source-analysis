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

### 1.6 randomSplit

```scala
def randomSplit(
      weights: Array[Double],
      seed: Long = Utils.random.nextLong): Array[RDD[T]] 
```

&emsp;&emsp;该函数根据`weights`权重，将一个`RDD`切分成多个`RDD`。该权重参数为一个`Double`数组，第二个参数为`random`的种子。

```scala
scala> var rdd = sc.makeRDD(1 to 10,10)
rdd: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[16] at makeRDD at :21
 
scala> rdd.collect
res6: Array[Int] = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)  
 
scala> var splitRDD = rdd.randomSplit(Array(1.0,2.0,3.0,4.0))
splitRDD: Array[org.apache.spark.rdd.RDD[Int]] = Array(MapPartitionsRDD[17] at randomSplit at :23, 
MapPartitionsRDD[18] at randomSplit at :23, 
MapPartitionsRDD[19] at randomSplit at :23, 
MapPartitionsRDD[20] at randomSplit at :23)
 
//这里注意：randomSplit的结果是一个RDD数组
scala> splitRDD.size
res8: Int = 4
//由于randomSplit的第一个参数weights中传入的值有4个，因此，就会切分成4个RDD,
//把原来的rdd按照权重1.0,2.0,3.0,4.0，随机划分到这4个RDD中，权重高的RDD，划分到的几率就大一些。
//注意，权重的总和加起来为1，否则会不正常
 
scala> splitRDD(0).collect
res10: Array[Int] = Array(1, 4)
 
scala> splitRDD(1).collect
res11: Array[Int] = Array(3)                                                    
 
scala> splitRDD(2).collect
res12: Array[Int] = Array(5, 9)
 
scala> splitRDD(3).collect
res13: Array[Int] = Array(2, 6, 7, 8, 10)
```

### 1.7 glom

```scala
def glom(): RDD[Array[T]]
```

&emsp;&emsp;该函数是将`RDD`中每一个分区中所有元素转换成数组，这样每一个分区就只有一个数组元素。

```scala
scala> var rdd = sc.makeRDD(1 to 10,3)
rdd: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[38] at makeRDD at :21
scala> rdd.partitions.size
res33: Int = 3  //该RDD有3个分区
scala> rdd.glom().collect
res35: Array[Array[Int]] = Array(Array(1, 2, 3), Array(4, 5, 6), Array(7, 8, 9, 10))
//glom将每个分区中的元素放到一个数组中，这样，结果就变成了3个数组
```