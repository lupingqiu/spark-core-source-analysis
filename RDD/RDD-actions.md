# RDD的Action操作

### 1 first

```scala
def first(): T
```
&emsp;&emsp;返回`RDD`的第一个元素。

```scala
scala> var rdd1 = sc.makeRDD(Array(("A","1"),("B","2"),("C","3")),2)
rdd1: org.apache.spark.rdd.RDD[(String, String)] = ParallelCollectionRDD[33] at makeRDD at :21
 
scala> rdd1.first
res14: (String, String) = (A,1)
 
scala> var rdd1 = sc.makeRDD(Seq(10, 4, 2, 12, 3))
rdd1: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[0] at makeRDD at :21
 
scala> rdd1.first
res8: Int = 10
```

### 2 count

```scala
def count(): Long
```
&emsp;&emsp;返回`RDD`中的元素数量。

```scala
scala> var rdd1 = sc.makeRDD(Array(("A","1"),("B","2"),("C","3")),2)
rdd1: org.apache.spark.rdd.RDD[(String, String)] = ParallelCollectionRDD[34] at makeRDD at :21
 
scala> rdd1.count
res15: Long = 3
```

### 3 reduce

```scala
def reduce(f: (T, T) ⇒ T): T
```
&emsp;&emsp;根据映射函数`f`，对`RDD`中的元素进行二元计算，返回计算结果。

```scala
scala> var rdd1 = sc.makeRDD(1 to 10,2)
rdd1: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[36] at makeRDD at :21
 
scala> rdd1.reduce(_ + _)
res18: Int = 55
 
scala> var rdd2 = sc.makeRDD(Array(("A",0),("A",2),("B",1),("B",2),("C",1)))
rdd2: org.apache.spark.rdd.RDD[(String, Int)] = ParallelCollectionRDD[38] at makeRDD at :21
 
scala> rdd2.reduce((x,y) => {
     |       (x._1 + y._1,x._2 + y._2)
     |     })
res21: (String, Int) = (CBBAA,6)
```

### 4 collect

```scala
def collect(): Array[T]
```
&emsp;&emsp;`collect`用于将一个`RDD`转换成数组。

```scala
scala> var rdd1 = sc.makeRDD(1 to 10,2)
rdd1: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[36] at makeRDD at :21
 
scala> rdd1.collect
res23: Array[Int] = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
```

### 5 top

```scala
def top(num: Int)(implicit ord: Ordering[T]): Array[T]
```
&emsp;&emsp;`top`函数用于从`RDD`中，按照默认（降序）或者指定的排序规则，返回前`num`个元素。

```scala
scala> var rdd1 = sc.makeRDD(Seq(10, 4, 2, 12, 3))
rdd1: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[40] at makeRDD at :21
 
scala> rdd1.top(1)
res2: Array[Int] = Array(12)
 
scala> rdd1.top(2)
res3: Array[Int] = Array(12, 10)
 
//指定排序规则
scala> implicit val myOrd = implicitly[Ordering[Int]].reverse
myOrd: scala.math.Ordering[Int] = scala.math.Ordering$$anon$4@767499ef
 
scala> rdd1.top(1)
res4: Array[Int] = Array(2)
 
scala> rdd1.top(2)
res5: Array[Int] = Array(2, 3)
```

### 6 take

```scala
def take(num: Int): Array[T]
```
&emsp;&emsp;`take`用于获取`RDD`中`num`个元素，不排序。

```scala
scala> var rdd1 = sc.makeRDD(Seq(10, 4, 2, 12, 3))
rdd1: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[40] at makeRDD at :21

scala> rdd1.take(1)
res0: Array[Int] = Array(10)

scala> rdd1.take(2)
res1: Array[Int] = Array(10, 4)
```

### 7 takeOrdered

```scala
def takeOrdered(num: Int)(implicit ord: Ordering[T]): Array[T]
```
&emsp;&emsp;`takeOrdered`和`top`类似，只不过以和`top`相反的顺序返回元素。

```scala
scala> var rdd1 = sc.makeRDD(Seq(10, 4, 2, 12, 3))
rdd1: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[40] at makeRDD at :21

scala> rdd1.top(1)
res4: Array[Int] = Array(2)

scala> rdd1.top(2)
res5: Array[Int] = Array(2, 3)

scala> rdd1.takeOrdered(1)
res6: Array[Int] = Array(12)

scala> rdd1.takeOrdered(2)
res7: Array[Int] = Array(12, 10)
```

### 8 aggregate

```scala
def aggregate[U: ClassTag](zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U): U
```
&emsp;&emsp;`aggregate`用于聚合`RDD`中的元素，先使用`seqOp`将`RDD`中每个分区中的T类型元素聚合成`U`类型，再使用`combOp`将之前每个分区聚合后的`U`类型聚合成`U`类型，特别注意`seqOp`和`combOp`都会使用`zeroValue`的值，`zeroValue`的类型为`U`。

```scala
var rdd1 = sc.makeRDD(1 to 10,2)
//第一个分区中包含5,4,3,2,1
//第二个分区中包含10,9,8,7,6
scala> rdd1.aggregate(1)(
     |           {(x : Int,y : Int) => x + y}, 
     |           {(a : Int,b : Int) => a + b}
     |     )
res17: Int = 58
```
&emsp;&emsp;结果为什么是58，看下面的计算过程：

- 在`part_0`中 `zeroValue+5+4+3+2+1 = 1+5+4+3+2+1 = 16`
- 在`part_1`中 `zeroValue+10+9+8+7+6 = 1+10+9+8+7+6 = 41`
- 再将两个分区的结果合并`(a : Int,b : Int) => a + b` ，并且使用`zeroValue`的值1 , 即`zeroValue+part_0+part_1 = 1 + 16 + 41 = 58`

&emsp;&emsp;`zeroValue`即确定了`U`的类型，也会对结果产生至关重要的影响，使用时候要特别注意。

### 9 fold

```scala
def fold(zeroValue: T)(op: (T, T) ⇒ T): T
```

&emsp;&emsp;`fold`是`aggregate`的简化，将`aggregate`中的`seqOp`和`combOp`使用同一个函数op。

```scala
scala> rdd1.fold(1)(
     |       (x,y) => x + y    
     |     )
res19: Int = 58
 
//结果同上面使用aggregate的第一个例子一样，即：
scala> rdd1.aggregate(1)(
     |           {(x,y) => x + y}, 
     |           {(a,b) => a + b}
     |     )
res20: Int = 58
```

### 10 lookup

```scala
def lookup(key: K): Seq[V]
```
&emsp;&emsp;`lookup`用于`(K,V)`类型的`RDD`,指定`K`值，返回`RDD`中该`K`对应的所有`V`值。
            
```scala
scala> var rdd1 = sc.makeRDD(Array(("A",0),("A",2),("B",1),("B",2),("C",1)))
rdd1: org.apache.spark.rdd.RDD[(String, Int)] = ParallelCollectionRDD[0] at makeRDD at :21
 
scala> rdd1.lookup("A")
res0: Seq[Int] = WrappedArray(0, 2)
 
scala> rdd1.lookup("B")
res1: Seq[Int] = WrappedArray(1, 2)
```

### 11 countByKey

```scala
def countByKey(): Map[K, Long]
```
&emsp;&emsp;`countByKey`用于统计`RDD[K,V]`中每个`K`的数量。

```scala
scala> var rdd1 = sc.makeRDD(Array(("A",0),("A",2),("B",1),("B",2),("B",3)))
rdd1: org.apache.spark.rdd.RDD[(String, Int)] = ParallelCollectionRDD[7] at makeRDD at :21

scala> rdd1.countByKey
res5: scala.collection.Map[String,Long] = Map(A -> 2, B -> 3)
```

### 12 foreach

```scala
def foreach(f: (T) => Unit): Unit
```
&emsp;&emsp;`foreach`用于遍历`RDD`,将函数`f`应用于每一个元素。
            
```scala
scala> var cnt = sc.accumulator(0)
cnt: org.apache.spark.Accumulator[Int] = 0
 
scala> var rdd1 = sc.makeRDD(1 to 10,2)
rdd1: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[5] at makeRDD at :21
 
scala> rdd1.foreach(x => cnt += x)
 
scala> cnt.value
res51: Int = 55
```

### 13 foreachPartition

```scala
def foreachPartition(f: (Iterator[T]) => Unit): Unit
```
&emsp;&emsp;`foreachPartition`和`foreach`类似，只不过是对每一个分区使用`f`。

```scala
scala> var rdd1 = sc.makeRDD(1 to 10,2)
rdd1: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[5] at makeRDD at :21
 
scala> var allsize = sc.accumulator(0)
size: org.apache.spark.Accumulator[Int] = 0
 
scala> var rdd1 = sc.makeRDD(1 to 10,2)
rdd1: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[6] at makeRDD at :21
 
scala>     rdd1.foreachPartition { x => {
     |       allsize += x.size
     |     }}
 
scala> println(allsize.value)
10
```

### 14 sortBy

```scala
def sortBy[K](f: (T) => K, ascending: Boolean = true, numPartitions: Int = this.partitions.length)(implicit ord: Ordering[K], ctag: ClassTag[K]): RDD[T]
```

&emsp;&emsp;`sortBy`根据给定的排序`k`函数将`RDD`中的元素进行排序。

```scala
scala> var rdd1 = sc.makeRDD(Seq(3,6,7,1,2,0),2)

scala> rdd1.sortBy(x => x).collect
res1: Array[Int] = Array(0, 1, 2, 3, 6, 7) //默认升序

scala> rdd1.sortBy(x => x,false).collect
res2: Array[Int] = Array(7, 6, 3, 2, 1, 0)  //降序

//RDD[K,V]类型
scala>var rdd1 = sc.makeRDD(Array(("A",2),("A",1),("B",6),("B",3),("B",7)))

scala> rdd1.sortBy(x => x).collect
res3: Array[(String, Int)] = Array((A,1), (A,2), (B,3), (B,6), (B,7))

//按照V进行降序排序
scala> rdd1.sortBy(x => x._2,false).collect
res4: Array[(String, Int)] = Array((B,7), (B,6), (B,3), (A,2), (A,1))
```

### 15 saveAsTextFile 与 saveAsSequenceFile

```scala
def saveAsTextFile(path: String): Unit
def saveAsTextFile(path: String, codec: Class[_ <: CompressionCodec]): Unit
```

&emsp;&emsp;`saveAsTextFile`用于将`RDD`以文本文件的格式存储到文件系统中。`codec`参数指定压缩的类名。

```scala
var rdd1 = sc.makeRDD(1 to 10,2)
rdd1.saveAsTextFile("hdfs://cdh5/tmp/1234.com/") //保存到HDFS
rdd1.saveAsTextFile(“file:///tmp/1234.com”)  //保存到本地文件
//指定压缩格式
rdd1.saveAsTextFile("hdfs://cdh5/tmp/1234.com/",classOf[com.hadoop.compression.lzo.LzopCodec])
```
&emsp;&emsp;`saveAsSequenceFile`用法和`saveAsTextFile`相同。

### 16 saveAsObjectFile

```scala
def saveAsObjectFile(path: String): Unit
```
&emsp;&emsp;`saveAsObjectFile`用于将`RDD`中的元素序列化成对象，存储到文件中。对于`HDFS`，默认采用`SequenceFile`保存。

```scala
var rdd1 = sc.makeRDD(1 to 10,2)
rdd1.saveAsObjectFile("hdfs://cdh5/tmp/1234.com/")
```

### 17 saveAsHadoopFile
                                        
```scala
def saveAsHadoopFile(path: String, keyClass: Class[_], valueClass: Class[_], outputFormatClass: Class[_ <: OutputFormat[_, _]], codec: Class[_ <: CompressionCodec]): Unit
def saveAsHadoopFile(path: String, keyClass: Class[_], valueClass: Class[_], outputFormatClass: Class[_ <: OutputFormat[_, _]], conf: JobConf = …, codec: Option[Class[_ <: CompressionCodec]] = None): Unit
```

&emsp;&emsp;`saveAsHadoopFile`是将`RDD`存储在`HDFS`上的文件中，支持老版本`Hadoop API`。可以指定`outputKeyClass`、`outputValueClass`以及压缩格式。每个分区输出一个文件。

```scala
var rdd1 = sc.makeRDD(Array(("A",2),("A",1),("B",6),("B",3),("B",7)))
 
import org.apache.hadoop.mapred.TextOutputFormat
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable
 
rdd1.saveAsHadoopFile("/tmp/1234.com/",classOf[Text],classOf[IntWritable],classOf[TextOutputFormat[Text,IntWritable]])
 
rdd1.saveAsHadoopFile("/tmp/1234.com/",classOf[Text],classOf[IntWritable],classOf[TextOutputFormat[Text,IntWritable]],
                   classOf[com.hadoop.compression.lzo.LzopCodec])
```

### 18 saveAsHadoopDataset

```scala
def saveAsHadoopDataset(conf: JobConf): Unit
```

&emsp;&emsp;`saveAsHadoopDataset`用于将`RDD`保存到`HDFS`以及其他存储中，比如`HBase`。
在`JobConf`中，通常需要关注或者设置五个参数文件的保存路径、`key`值的`class`类型、`value`值的`class`类型、`RDD`的输出格式(`OutputFormat`)、以及压缩相关的参数。

&emsp;&emsp;下面的例子将`RDD`保存到`HDFS`中。

```scala
var rdd1 = sc.makeRDD(Array(("A",2),("A",1),("B",6),("B",3),("B",7)))
var jobConf = new JobConf()
jobConf.setOutputFormat(classOf[TextOutputFormat[Text,IntWritable]])
jobConf.setOutputKeyClass(classOf[Text])
jobConf.setOutputValueClass(classOf[IntWritable])
jobConf.set("mapred.output.dir","/tmp/1234.com/")
rdd1.saveAsHadoopDataset(jobConf)
```
&emsp;&emsp;下面的例子将数据保存到`hbase`。

```scala
var conf = HBaseConfiguration.create()
var jobConf = new JobConf(conf)
jobConf.set("hbase.zookeeper.quorum","zkNode1,zkNode2,zkNode3")
jobConf.set("zookeeper.znode.parent","/hbase")
jobConf.set(TableOutputFormat.OUTPUT_TABLE,"1234")
jobConf.setOutputFormat(classOf[TableOutputFormat])
var rdd1 = sc.makeRDD(Array(("A",2),("B",6),("C",7)))
rdd1.map(x => 
    {
        var put = new Put(Bytes.toBytes(x._1))
        put.add(Bytes.toBytes("f1"), Bytes.toBytes("c1"), Bytes.toBytes(x._2))
        (new ImmutableBytesWritable,put)
    }
).saveAsHadoopDataset(jobConf)
```

### 19 saveAsNewAPIHadoopFile

```scala
def saveAsNewAPIHadoopFile[F <: OutputFormat[K, V]](path: String)(implicit fm: ClassTag[F]): Unit
def saveAsNewAPIHadoopFile(path: String, keyClass: Class[_], valueClass: Class[_], outputFormatClass: Class[_ <: OutputFormat[_, _]], conf: Configuration = self.context.hadoopConfiguration): Unit
```

&emsp;&emsp;`saveAsNewAPIHadoopFile`用于将`RDD`数据保存到`HDFS`上，使用新版本`Hadoop API`。

```scala
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import SparkContext._
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable
 
var rdd1 = sc.makeRDD(Array(("A",2),("A",1),("B",6),("B",3),("B",7)))
rdd1.saveAsNewAPIHadoopFile("/tmp/1234/",classOf[Text],classOf[IntWritable],classOf[TextOutputFormat[Text,IntWritable]])
```

### 20 saveAsNewAPIHadoopDataset

```scala
def saveAsNewAPIHadoopDataset(conf: Configuration): Unit
```

&emsp;&emsp;作用同`saveAsHadoopDataset`,采用新版本`Hadoop API`。

```scala
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import SparkContext._
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Put
 
object Test {
  def main(args : Array[String]) {
   val sparkConf = new SparkConf().setMaster("spark://1234.com:7077").setAppName("1234.com")
   val sc = new SparkContext(sparkConf);
   var rdd1 = sc.makeRDD(Array(("A",2),("B",6),("C",7)))
   
    sc.hadoopConfiguration.set("hbase.zookeeper.quorum ","zkNode1,zkNode2,zkNode3")
    sc.hadoopConfiguration.set("zookeeper.znode.parent","/hbase")
    sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE,"1234")
    var job = new Job(sc.hadoopConfiguration)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    
    rdd1.map(
      x => {
        var put = new Put(Bytes.toBytes(x._1))
        put.add(Bytes.toBytes("f1"), Bytes.toBytes("c1"), Bytes.toBytes(x._2))
        (new ImmutableBytesWritable,put)
      }    
    ).saveAsNewAPIHadoopDataset(job.getConfiguration)

    sc.stop()
  }
}
```

## 参考资料

[Spark算子系列文章](http://lxw1234.com/archives/2015/07/363.htm)