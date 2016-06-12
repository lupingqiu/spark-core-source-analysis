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

### 1.8 union

```scala
def union(other: RDD[T]): RDD[T]
```
&emsp;&emsp;该函数比较简单，就是将两个RDD进行合并，不去重。

```scala
scala> var rdd1 = sc.makeRDD(1 to 2,1)
rdd1: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[45] at makeRDD at :21
 
scala> rdd1.collect
res42: Array[Int] = Array(1, 2)
 
scala> var rdd2 = sc.makeRDD(2 to 3,1)
rdd2: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[46] at makeRDD at :21
 
scala> rdd2.collect
res43: Array[Int] = Array(2, 3)
 
scala> rdd1.union(rdd2).collect
res44: Array[Int] = Array(1, 2, 2, 3)
```

### 1.9 intersection

```scala
def intersection(other: RDD[T]): RDD[T]
def intersection(other: RDD[T], numPartitions: Int): RDD[T]
def intersection(other: RDD[T], partitioner: Partitioner)(implicit ord: Ordering[T] = null): RDD[T]
```

&emsp;&emsp;该函数返回两个`RDD`的交集，并且去重。参数`numPartitions`指定返回的`RDD`的分区数。参数`partitioner`用于指定分区函数。

```scala
scala> var rdd1 = sc.makeRDD(1 to 2,1)
rdd1: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[45] at makeRDD at :21
 
scala> rdd1.collect
res42: Array[Int] = Array(1, 2)
 
scala> var rdd2 = sc.makeRDD(2 to 3,1)
rdd2: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[46] at makeRDD at :21
 
scala> rdd2.collect
res43: Array[Int] = Array(2, 3)
 
scala> rdd1.intersection(rdd2).collect
res45: Array[Int] = Array(2)
 
scala> var rdd3 = rdd1.intersection(rdd2)
rdd3: org.apache.spark.rdd.RDD[Int] = MapPartitionsRDD[59] at intersection at :25
 
scala> rdd3.partitions.size
res46: Int = 1
 
scala> var rdd3 = rdd1.intersection(rdd2,2)
rdd3: org.apache.spark.rdd.RDD[Int] = MapPartitionsRDD[65] at intersection at :25
 
scala> rdd3.partitions.size
res47: Int = 2
```

### 1.10 subtract

```scala
def subtract(other: RDD[T]): RDD[T]
def subtract(other: RDD[T], numPartitions: Int): RDD[T]
def subtract(other: RDD[T], partitioner: Partitioner)(implicit ord: Ordering[T] = null): RDD[T]
```

&emsp;&emsp;该函数类似于`intersection`，但返回在`RDD`中出现，并且不在`otherRDD`中出现的元素，不去重。

```scala
scala> var rdd1 = sc.makeRDD(Seq(1,2,2,3))
rdd1: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[66] at makeRDD at :21

scala> rdd1.collect
res48: Array[Int] = Array(1, 2, 2, 3)

scala> var rdd2 = sc.makeRDD(3 to 4)
rdd2: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[67] at makeRDD at :21

scala> rdd2.collect
res49: Array[Int] = Array(3, 4)

scala> rdd1.subtract(rdd2).collect
res50: Array[Int] = Array(1, 2, 2)
```

### 1.11 mapPartitions

```scala
def mapPartitions[U: ClassTag](
      f: Iterator[T] => Iterator[U],
      preservesPartitioning: Boolean = false): RDD[U]
```

&emsp;&emsp;该函数和`map`函数类似，只不过映射函数的参数由`RDD`中的每一个元素变成了`RDD`中每一个分区的迭代器。如果在映射的过程中需要频繁创建额外的对象，使用`mapPartitions`要比`map`高效的多。

&emsp;&emsp;比如，将`RDD`中的所有数据通过`JDBC`连接写入数据库，如果使用`map`函数，可能要为每一个元素都创建一个`connection`，这样开销很大，如果使用`mapPartitions`，那么只需要针对每一个分区建立一个`connection`。

&emsp;&emsp;参数`preservesPartitioning`表示是否保留父`RDD`的`partitioner`分区信息。

```scala
var rdd1 = sc.makeRDD(1 to 5,2)
//rdd1有两个分区
scala> var rdd3 = rdd1.mapPartitions{ x => {
     | var result = List[Int]()
     |     var i = 0
     |     while(x.hasNext){
     |       i += x.next()
     |     }
     |     result.::(i).iterator
     | }}
rdd3: org.apache.spark.rdd.RDD[Int] = MapPartitionsRDD[84] at mapPartitions at :23
 
//rdd3将rdd1中每个分区中的数值累加
scala> rdd3.collect
res65: Array[Int] = Array(3, 12)
scala> rdd3.partitions.size
res66: Int = 2
```

### 1.12 mapPartitionsWithIndex

```scala
def mapPartitionsWithIndex[U: ClassTag](
      f: (Int, Iterator[T]) => Iterator[U],
      preservesPartitioning: Boolean = false): RDD[U]
```

&emsp;&emsp;函数作用同`mapPartitions`相同，不过提供了两个参数，第一个参数为分区的索引。

```scala
var rdd1 = sc.makeRDD(1 to 5,2)
//rdd1有两个分区
var rdd2 = rdd1.mapPartitionsWithIndex{
        (x,iter) => {
          var result = List[String]()
            var i = 0
            while(iter.hasNext){
              i += iter.next()
            }
            result.::(x + "|" + i).iterator

        }
      }
//rdd2将rdd1中每个分区的数字累加，并在每个分区的累加结果前面加了分区索引
scala> rdd2.collect
res13: Array[String] = Array(0|3, 1|12)
```

### 1.13 zip

```scala
  def zip[U: ClassTag](other: RDD[U]): RDD[(T, U)] 
```

&emsp;&emsp;`zip`函数用于将两个`RDD`组合成`Key/Value`形式的`RDD`,这里默认两个`RDD`的`partition`数量以及每个`partition`的元素数量都相同，否则会抛出异常。

```scala
scala> var rdd1 = sc.makeRDD(1 to 5,2)
rdd1: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[1] at makeRDD at :21
 
scala> var rdd2 = sc.makeRDD(Seq("A","B","C","D","E"),2)
rdd2: org.apache.spark.rdd.RDD[String] = ParallelCollectionRDD[2] at makeRDD at :21
 
scala> rdd1.zip(rdd2).collect
res0: Array[(Int, String)] = Array((1,A), (2,B), (3,C), (4,D), (5,E))           
 
scala> rdd2.zip(rdd1).collect
res1: Array[(String, Int)] = Array((A,1), (B,2), (C,3), (D,4), (E,5))
 
scala> var rdd3 = sc.makeRDD(Seq("A","B","C","D","E"),3)
rdd3: org.apache.spark.rdd.RDD[String] = ParallelCollectionRDD[5] at makeRDD at :21
 
scala> rdd1.zip(rdd3).collect
java.lang.IllegalArgumentException: Can't zip RDDs with unequal numbers of partitions
//如果两个RDD分区数不同，则抛出异常
```

### 1.14 zipPartitions

```scala
def zipPartitions[B: ClassTag, V: ClassTag]
      (rdd2: RDD[B], preservesPartitioning: Boolean)
      (f: (Iterator[T], Iterator[B]) => Iterator[V]): RDD[V] = withScope {
    new ZippedPartitionsRDD2(sc, sc.clean(f), this, rdd2, preservesPartitioning)
  }
def zipPartitions[B: ClassTag, C: ClassTag, V: ClassTag]
      (rdd2: RDD[B], rdd3: RDD[C], preservesPartitioning: Boolean)
      (f: (Iterator[T], Iterator[B], Iterator[C]) => Iterator[V]): RDD[V] = withScope {
    new ZippedPartitionsRDD3(sc, sc.clean(f), this, rdd2, rdd3, preservesPartitioning)
  }
def zipPartitions[B: ClassTag, C: ClassTag, D: ClassTag, V: ClassTag]
      (rdd2: RDD[B], rdd3: RDD[C], rdd4: RDD[D], preservesPartitioning: Boolean)
      (f: (Iterator[T], Iterator[B], Iterator[C], Iterator[D]) => Iterator[V]): RDD[V] = withScope {
    new ZippedPartitionsRDD4(sc, sc.clean(f), this, rdd2, rdd3, rdd4, preservesPartitioning)
  }
```

&emsp;&emsp;`zipPartitions`函数将多个`RDD`按照`partition`组合成为新的`RDD`，该函数需要组合的`RDD`具有相同的分区数，但对于每个分区内的元素数量没有要求。

```scala
val a = sc.parallelize(0 to 9, 3)
val b = sc.parallelize(10 to 19, 3)
val c = sc.parallelize(100 to 109, 3)
def myfunc(aiter: Iterator[Int], biter: Iterator[Int], citer: Iterator[Int]): Iterator[String] =
{
  var res = List[String]()
  while (aiter.hasNext && biter.hasNext && citer.hasNext)
  {
    val x = aiter.next + " " + biter.next + " " + citer.next
    res ::= x
  }
  res.iterator
}
a.zipPartitions(b, c)(myfunc).collect
res50: Array[String] = Array(2 12 102, 1 11 101, 0 10 100, 5 15 105, 4 14 104, 3 13 103, 9 19 109, 8 18 108, 7 17 107, 6 16 106)
```

### 1.15 zipWithIndex

```scala
def zipWithIndex(): RDD[(T, Long)]
```
&emsp;&emsp;该函数将`RDD`中的元素和这个元素在`RDD`中的`ID`（索引号）组合成键/值对。

```scala
val z = sc.parallelize(Array("A", "B", "C", "D"))
val r = z.zipWithIndex
res110: Array[(String, Long)] = Array((A,0), (B,1), (C,2), (D,3))

val z = sc.parallelize(100 to 120, 5)
val r = z.zipWithIndex
r.collect
res11: Array[(Int, Long)] = Array((100,0), (101,1), (102,2), (103,3), (104,4), (105,5), (106,6), (107,7), (108,8), (109,9), (110,10), (111,11), (112,12), (113,13), (114,14), (115,15), (116,16), (117,17), (118,18), (119,19), (120,20))
```

### 1.16 zipWithUniqueId

```scala
def zipWithUniqueId(): RDD[(T, Long)]
```

&emsp;&emsp;该函数将RDD中元素和一个唯一ID组合成键/值对，该唯一ID生成算法如下：

- 每个分区中第一个元素的唯一ID值为：该分区索引号
- 每个分区中第N个元素的唯一ID值为：(前一个元素的唯一ID值) + (该RDD总的分区数)

```scala
val z = sc.parallelize(100 to 120, 5)
val r = z.zipWithUniqueId
r.collect

res12: Array[(Int, Long)] = Array((100,0), (101,5), (102,10), (103,15), (104,1), (105,6), (106,11), (107,16), (108,2), (109,7), (110,12), (111,17), (112,3), (113,8), (114,13), (115,18), (116,4), (117,9), (118,14), (119,19), (120,24))
```

## 2 键值转换操作

### 2.1 partitionBy

```scala
def partitionBy(partitioner: Partitioner): RDD[(K, V)]
```

&emsp;&emsp;该函数根据`partitioner`函数生成新的`ShuffleRDD`，将原`RDD`重新分区。

```scala
scala> var rdd1 = sc.makeRDD(Array((1,"A"),(2,"B"),(3,"C"),(4,"D")),2)
rdd1: org.apache.spark.rdd.RDD[(Int, String)] = ParallelCollectionRDD[23] at makeRDD at :21
 
scala> rdd1.partitions.size
res20: Int = 2
 
//查看rdd1中每个分区的元素
scala> rdd1.mapPartitionsWithIndex{
     |         (partIdx,iter) => {
     |           var part_map = scala.collection.mutable.Map[String,List[(Int,String)]]()
     |             while(iter.hasNext){
     |               var part_name = "part_" + partIdx;
     |               var elem = iter.next()
     |               if(part_map.contains(part_name)) {
     |                 var elems = part_map(part_name)
     |                 elems ::= elem
     |                 part_map(part_name) = elems
     |               } else {
     |                 part_map(part_name) = List[(Int,String)]{elem}
     |               }
     |             }
     |             part_map.iterator
     |            
     |         }
     |       }.collect
res22: Array[(String, List[(Int, String)])] = Array((part_0,List((2,B), (1,A))), (part_1,List((4,D), (3,C))))
//(2,B),(1,A)在part_0中，(4,D),(3,C)在part_1中

//使用partitionBy重分区
scala> var rdd2 = rdd1.partitionBy(new org.apache.spark.HashPartitioner(2))
rdd2: org.apache.spark.rdd.RDD[(Int, String)] = ShuffledRDD[25] at partitionBy at :23
 
scala> rdd2.partitions.size
res23: Int = 2
 
//查看rdd2中每个分区的元素
scala> rdd2.mapPartitionsWithIndex{
     |         (partIdx,iter) => {
     |           var part_map = scala.collection.mutable.Map[String,List[(Int,String)]]()
     |             while(iter.hasNext){
     |               var part_name = "part_" + partIdx;
     |               var elem = iter.next()
     |               if(part_map.contains(part_name)) {
     |                 var elems = part_map(part_name)
     |                 elems ::= elem
     |                 part_map(part_name) = elems
     |               } else {
     |                 part_map(part_name) = List[(Int,String)]{elem}
     |               }
     |             }
     |             part_map.iterator
     |         }
     |       }.collect
res24: Array[(String, List[(Int, String)])] = Array((part_0,List((4,D), (2,B))), (part_1,List((3,C), (1,A))))
//(4,D),(2,B)在part_0中，(3,C),(1,A)在part_1中
```

### 2.2 mapValues

```scala
def mapValues[U](f: V => U): RDD[(K, U)]
```
&emsp;&emsp;同基本转换操作中的`map`类似，只不过`mapValues`是针对`[K,V]`中的`V`值进行`map`操作。

```scala
scala> var rdd1 = sc.makeRDD(Array((1,"A"),(2,"B"),(3,"C"),(4,"D")),2)
rdd1: org.apache.spark.rdd.RDD[(Int, String)] = ParallelCollectionRDD[27] at makeRDD at :21
 
scala> rdd1.mapValues(x => x + "_").collect
res26: Array[(Int, String)] = Array((1,A_), (2,B_), (3,C_), (4,D_))
```

### 2.3 flatMapValues

```scala
def flatMapValues[U](f: (V) => TraversableOnce[U]): RDD[(K, U)]
```

&emsp;&emsp;同基本转换操作中的`flatMap`类似，只不过`flatMapValues`是针对`[K,V]`中的`V`值进行`flatMap`操作。

```scala
scala> rdd1.flatMapValues(x => x + "_").collect
res36: Array[(Int, Char)] = Array((1,A), (1,_), (2,B), (2,_), (3,C), (3,_), (4,D), (4,_))
```

### 2.4 combineByKey

```scala
def combineByKey[C](createCombiner: V => C, mergeValue: (C, V) => C, mergeCombiners: (C, C) => C): RDD[(K, C)]
def combineByKey[C](createCombiner: V => C, mergeValue: (C, V) => C, mergeCombiners: (C, C) => C, numPartitions: Int): RDD[(K, C)]
def combineByKey[C](createCombiner: V => C, mergeValue: (C, V) => C, mergeCombiners: (C, C) => C, partitioner: Partitioner, mapSideCombine: Boolean = true, serializer: Serializer = null): RDD[(K, C)]
```

&emsp;&emsp;该函数用于将`RDD[K,V]`转换成`RDD[K,C]`,这里的`V`类型和`C`类型可以相同也可以不同。

&emsp;&emsp;参数的含义如下：

- `createCombiner`：组合器函数，用于将`V`类型转换成`C`类型，输入参数为`RDD[K,V]`中的`V`,输出为`C`
- `mergeValue`：合并值函数，将一个`C`类型和一个`V`类型值合并成一个`C`类型，输入参数为`(C,V)`，输出为`C`
- `mergeCombiners`：合并组合器函数，用于将两个`C`类型值合并成一个`C`类型，输入参数为`(C,C)`，输出为`C`
- `numPartitions`：结果`RDD`分区数，默认保持原有的分区数
- `partitioner`：分区函数,默认为`HashPartitioner`
- `mapSideCombine`：是否需要在`Map`端进行`combine`操作，类似于`MapReduce`中的`combine`，默认为`true`
- `serializer`：序列化类，默认为`null`

```scala
scala> var rdd1 = sc.makeRDD(Array(("A",1),("A",2),("B",1),("B",2),("C",1)))
rdd1: org.apache.spark.rdd.RDD[(String, Int)] = ParallelCollectionRDD[64] at makeRDD at :21
 
scala> rdd1.combineByKey(
     |       (v : Int) => v + "_",   
     |       (c : String, v : Int) => c + "@" + v,  
     |       (c1 : String, c2 : String) => c1 + "$" + c2
     |     ).collect
res60: Array[(String, String)] = Array((A,2_$1_), (B,1_$2_), (C,1_))
```
&emsp;&emsp;其中三个映射函数分别为：
- createCombiner: (V) => C

&emsp;&emsp;`(v : Int) => v + “_”` ,在每一个V值后面加上字符_，返回C类型(String)
- mergeValue: (C, V) => C

&emsp;&emsp;`(c : String, v : Int) => c + “@” + v` ,合并C类型和V类型，中间加字符@,返回C(String)
- mergeCombiners: (C, C) => C

&emsp;&emsp;`(c1 : String, c2 : String) => c1 + “$” + c2` ,合并C类型和C类型，中间加$，返回C(String)

&emsp;&emsp;其他参数为默认值。

&emsp;&emsp;最终，将`RDD[String,Int]`转换为`RDD[String,String]`。

### 2.5 foldByKey

```scala
def foldByKey(zeroValue: V)(func: (V, V) => V): RDD[(K, V)]
def foldByKey(zeroValue: V, numPartitions: Int)(func: (V, V) => V): RDD[(K, V)]
def foldByKey(zeroValue: V, partitioner: Partitioner)(func: (V, V) => V): RDD[(K, V)]
```

&emsp;&emsp;该函数用于`RDD[K,V]`根据`K`将`V`做折叠、合并处理，其中的参数`zeroValue`表示先根据映射函数将`zeroValue`应用于`V`,对`V`进行初始化,再将映射函数应用于初始化后的`V`。

```scala
scala> var rdd1 = sc.makeRDD(Array(("A",0),("A",2),("B",1),("B",2),("C",1)))
scala> rdd1.foldByKey(0)(_+_).collect
res75: Array[(String, Int)] = Array((A,2), (B,3), (C,1)) 
//将rdd1中每个key对应的V进行累加，注意zeroValue=0,需要先初始化V,映射函数为+操
//作，比如("A",0), ("A",2)，先将zeroValue应用于每个V,得到：("A",0+0), ("A",2+0)，即：
//("A",0), ("A",2)，再将映射函数应用于初始化后的V，最后得到(A,0+2),即(A,2)
```

&emsp;&emsp;再看下面的例子：

```scala
scala> rdd1.foldByKey(2)(_+_).collect
res76: Array[(String, Int)] = Array((A,6), (B,7), (C,3))
//先将zeroValue=2应用于每个V,得到：("A",0+2), ("A",2+2)，即：("A",2), ("A",4)，再将映射函
//数应用于初始化后的V，最后得到：(A,2+4)，即：(A,6)
```

### 2.6 groupByKey

```scala
def groupByKey(): RDD[(K, Iterable[V])]
def groupByKey(numPartitions: Int): RDD[(K, Iterable[V])]
def groupByKey(partitioner: Partitioner): RDD[(K, Iterable[V])]
```

&emsp;&emsp;该函数用于将`RDD[K,V]`中每个`K`对应的`V`值，合并到一个集合`Iterable[V]`中，参数`numPartitions`用于指定分区数；参数`partitioner`用于指定分区函数。
该方法代价较高，在求每个`key`的`sum`或者`average`时，推荐使用`reduceByKey`或者`aggregateByKey`。

```scala
scala> var rdd1 = sc.makeRDD(Array(("A",0),("A",2),("B",1),("B",2),("C",1)))
rdd1: org.apache.spark.rdd.RDD[(String, Int)] = ParallelCollectionRDD[89] at makeRDD at :21
 
scala> rdd1.groupByKey().collect
res81: Array[(String, Iterable[Int])] = Array((A,CompactBuffer(0, 2)), (B,CompactBuffer(2, 1)), (C,CompactBuffer(1)))
```

### 2.7 reduceByKey

```scala
def reduceByKey(func: (V, V) => V): RDD[(K, V)]
def reduceByKey(func: (V, V) => V, numPartitions: Int): RDD[(K, V)]
def reduceByKey(partitioner: Partitioner, func: (V, V) => V): RDD[(K, V)]
```

&emsp;&emsp;该函数用于将`RDD[K,V]`中每个`K`对应的`V`值根据映射函数来运算。参数`numPartitions`用于指定分区数；参数`partitioner`用于指定分区函数。

```scala
scala> var rdd1 = sc.makeRDD(Array(("A",0),("A",2),("B",1),("B",2),("C",1)))
rdd1: org.apache.spark.rdd.RDD[(String, Int)] = ParallelCollectionRDD[91] at makeRDD at :21
 
scala> rdd1.partitions.size
res82: Int = 15
 
scala> var rdd2 = rdd1.reduceByKey((x,y) => x + y)
rdd2: org.apache.spark.rdd.RDD[(String, Int)] = ShuffledRDD[94] at reduceByKey at :23
 
scala> rdd2.collect
res85: Array[(String, Int)] = Array((A,2), (B,3), (C,1))
 
scala> rdd2.partitions.size
res86: Int = 15
 
scala> var rdd2 = rdd1.reduceByKey(new org.apache.spark.HashPartitioner(2),(x,y) => x + y)
rdd2: org.apache.spark.rdd.RDD[(String, Int)] = ShuffledRDD[95] at reduceByKey at :23
 
scala> rdd2.collect
res87: Array[(String, Int)] = Array((B,3), (A,2), (C,1))
 
scala> rdd2.partitions.size
res88: Int = 2
```

### 2.8 reduceByKeyLocally

```scala
def reduceByKeyLocally(func: (V, V) => V): Map[K, V]
```

&emsp;&emsp;该函数将`RDD[K,V]`中每个`K`对应的`V`值根据映射函数来运算，运算结果映射到一个`Map[K,V]`中，而不是`RDD[K,V]`。

```scala
scala> var rdd1 = sc.makeRDD(Array(("A",0),("A",2),("B",1),("B",2),("C",1)))
rdd1: org.apache.spark.rdd.RDD[(String, Int)] = ParallelCollectionRDD[91] at makeRDD at :21

scala> rdd1.reduceByKeyLocally((x,y) => x + y)
res90: scala.collection.Map[String,Int] = Map(B -> 3, A -> 2, C -> 1)
```

### 2.9 cogroup

```scala
//参数为1个RDD
def cogroup[W](other: RDD[(K, W)]): RDD[(K, (Iterable[V], Iterable[W]))]
def cogroup[W](other: RDD[(K, W)], numPartitions: Int): RDD[(K, (Iterable[V], Iterable[W]))]
def cogroup[W](other: RDD[(K, W)], partitioner: Partitioner): RDD[(K, (Iterable[V], Iterable[W]))]

//参数为2个RDD
def cogroup[W1, W2](other1: RDD[(K, W1)], other2: RDD[(K, W2)]): RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2]))]
def cogroup[W1, W2](other1: RDD[(K, W1)], other2: RDD[(K, W2)], numPartitions: Int): RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2]))]
def cogroup[W1, W2](other1: RDD[(K, W1)], other2: RDD[(K, W2)], partitioner: Partitioner): RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2]))]

//参数为3个RDD
def cogroup[W1, W2, W3](other1: RDD[(K, W1)], other2: RDD[(K, W2)], other3: RDD[(K, W3)]): RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3]))]
def cogroup[W1, W2, W3](other1: RDD[(K, W1)], other2: RDD[(K, W2)], other3: RDD[(K, W3)], numPartitions: Int): RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3]))]
def cogroup[W1, W2, W3](other1: RDD[(K, W1)], other2: RDD[(K, W2)], other3: RDD[(K, W3)], partitioner: Partitioner): RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3]))]
```

&emsp;&emsp;`cogroup`相当于`SQL`中的全外连接`full outer join`，返回左右`RDD`中的记录，关联不上的为空。参数`numPartitions`用于指定结果的分区数。参数`partitioner`用于指定分区函数。

```scala
var rdd1 = sc.makeRDD(Array(("A","1"),("B","2"),("C","3")),2)
var rdd2 = sc.makeRDD(Array(("A","a"),("C","c"),("D","d")),2)
 
scala> var rdd3 = rdd1.cogroup(rdd2)
rdd3: org.apache.spark.rdd.RDD[(String, (Iterable[String], Iterable[String]))] = MapPartitionsRDD[12] at cogroup at :25
 
scala> rdd3.partitions.size
res3: Int = 2
 
scala> rdd3.collect
res1: Array[(String, (Iterable[String], Iterable[String]))] = Array(
(B,(CompactBuffer(2),CompactBuffer())), 
(D,(CompactBuffer(),CompactBuffer(d))), 
(A,(CompactBuffer(1),CompactBuffer(a))), 
(C,(CompactBuffer(3),CompactBuffer(c)))
)
```

### 2.10 join

```scala
def join[W](other: RDD[(K, W)]): RDD[(K, (V, W))]
def join[W](other: RDD[(K, W)], numPartitions: Int): RDD[(K, (V, W))]
def join[W](other: RDD[(K, W)], partitioner: Partitioner): RDD[(K, (V, W))]
```
&emsp;&emsp;`join`相当于`SQL`中的内关联`join`，只返回两个`RDD`根据`K`可以关联上的结果，`join`只能用于两个`RDD`之间的关联，如果要多个`RDD`关联，多关联几次即可。

```scala
var rdd1 = sc.makeRDD(Array(("A","1"),("B","2"),("C","3")),2)
var rdd2 = sc.makeRDD(Array(("A","a"),("C","c"),("D","d")),2)
rdd1.join(rdd2).collect

res10: Array[(String, (String, String))] = Array((A,(1,a)), (C,(3,c)))
```

### 2.11 leftOuterJoin

```scala
def leftOuterJoin[W](other: RDD[(K, W)]): RDD[(K, (V, Option[W]))]
def leftOuterJoin[W](other: RDD[(K, W)], numPartitions: Int): RDD[(K, (V, Option[W]))]
def leftOuterJoin[W](other: RDD[(K, W)], partitioner: Partitioner): RDD[(K, (V, Option[W]))]
```

&emsp;&emsp;`leftOuterJoin`类似于`SQL`中的左外关联`left outer join`，返回结果以前面的`RDD`为主，关联不上的记录为空。只能用于两个`RDD`之间的关联，如果要多个`RDD`关联，多关联几次即可。
参数`numPartitions`用于指定结果的分区数参数`partitioner`用于指定分区函数。

```scala
var rdd1 = sc.makeRDD(Array(("A","1"),("B","2"),("C","3")),2)
var rdd2 = sc.makeRDD(Array(("A","a"),("C","c"),("D","d")),2)
 
rdd1.leftOuterJoin(rdd2).collect

res11: Array[(String, (String, Option[String]))] = Array((B,(2,None)), (A,(1,Some(a))), (C,(3,Some(c))))
```

### 2.12 rightOuterJoin

```scala
def rightOuterJoin[W](other: RDD[(K, W)]): RDD[(K, (Option[V], W))]
def rightOuterJoin[W](other: RDD[(K, W)], numPartitions: Int): RDD[(K, (Option[V], W))]
def rightOuterJoin[W](other: RDD[(K, W)], partitioner: Partitioner): RDD[(K, (Option[V], W))]
```
&emsp;&emsp;`rightOuterJoin`类似于`SQL`中的有外关联`right outer join`，返回结果以参数中的`RDD`为主，关联不上的记录为空。只能用于两个`RDD`之间的关联，如果要多个`RDD`关联，多关联几次即可。
参数`numPartitions`用于指定结果的分区数;参数`partitioner`用于指定分区函数。

```scala
var rdd1 = sc.makeRDD(Array(("A","1"),("B","2"),("C","3")),2)
var rdd2 = sc.makeRDD(Array(("A","a"),("C","c"),("D","d")),2)
rdd1.rightOuterJoin(rdd2).collect

res12: Array[(String, (Option[String], String))] = Array((D,(None,d)), (A,(Some(1),a)), (C,(Some(3),c)))
```

### 2.13 subtractByKey

```scala
def subtractByKey[W](other: RDD[(K, W)])(implicit arg0: ClassTag[W]): RDD[(K, V)]
def subtractByKey[W](other: RDD[(K, W)], numPartitions: Int)(implicit arg0: ClassTag[W]): RDD[(K, V)]
def subtractByKey[W](other: RDD[(K, W)], p: Partitioner)(implicit arg0: ClassTag[W]): RDD[(K, V)]
```

&emsp;&emsp;`subtractByKey`和基本转换操作中的`subtract`类似,只不过这里是针对`K`的，返回在主`RDD`中出现，并且不在`otherRDD`中出现的元素。参数`numPartitions`用于指定结果的分区数;参数`partitioner`用于指定分区函数。

```scala
var rdd1 = sc.makeRDD(Array(("A","1"),("B","2"),("C","3")),2)
var rdd2 = sc.makeRDD(Array(("A","a"),("C","c"),("D","d")),2)
rdd1.subtractByKey(rdd2).collect

res13: Array[(String, String)] = Array((B,2))
```

## 3 RDD的Action操作


## 参考资料

[Spark算子系列文章](http://lxw1234.com/archives/2015/07/363.htm)