## 键值转换操作

### 1 partitionBy

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

### 2 mapValues

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

### 3 flatMapValues

```scala
def flatMapValues[U](f: (V) => TraversableOnce[U]): RDD[(K, U)]
```

&emsp;&emsp;同基本转换操作中的`flatMap`类似，只不过`flatMapValues`是针对`[K,V]`中的`V`值进行`flatMap`操作。

```scala
scala> rdd1.flatMapValues(x => x + "_").collect
res36: Array[(Int, Char)] = Array((1,A), (1,_), (2,B), (2,_), (3,C), (3,_), (4,D), (4,_))
```

### 4 combineByKey

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

### 5 foldByKey

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

### 6 groupByKey

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

### 7 reduceByKey

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

### 8 reduceByKeyLocally

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

### 9 cogroup

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

### 10 join

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

### 11 leftOuterJoin

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

### 12 rightOuterJoin

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

### 13 subtractByKey

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

## 参考资料

[Spark算子系列文章](http://lxw1234.com/archives/2015/07/363.htm)