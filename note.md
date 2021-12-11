
Command | Description
--- | ---
ant|Build the default target (for simpledb, this is dist).
ant -projecthelp|List all the targets in `build.xml` with descriptions.
ant dist|Compile the code in src and package it in `dist/simpledb.jar`.
ant test|Compile and run all the unit tests.
ant runtest -Dtest=testname|Run the unit test named `testname`.
ant systemtest|Compile and run all the system tests.
ant runsystest -Dtest=testname|Compile and run the system test named `testname`.

### Lab1
#### 1.1: Tuple
- 所谓的iterator，可以借助ArraList实现，ArrayyList.itearor即iterator<T>,ArrayList没有初值初始化，必须写一个add方法循环初始化。
- String比较用equals
- 判断某个实例的类型用instanceof

#### 1.2：Catalog
- HashMap判断key存在用containsKey()方法，移除remove，插入put
- HashMap中的类型需要是包装类型，即不能是int，而是要Integer
- get方法中参数有可能是null时需要判断
- 声明了HashMap要在构造函数中初始化

#### 1.3：BufferPool
- getPage方法若对于page在，则返回，不在则需要读取一个page进来，但是若读取时pool满了，则需要换出一个页面（还未实现）

#### 1.4：HeapPage
- 注意HeapPageId和RecordId的equals的实现以及RecordId的hashcode的方法实现（用到pid.hashCode()方法）
- 计算numTuple和headerSize时注意ceil和floor方法要使用浮点数计算才有效，比如乘以1.0
- Iterator借助ArrayList实现，存储所有可用的Tuple

#### 1.5：HeapFile
- readPage()方法需要从磁盘文件读取内容，因此需要根据pageNum计算出page在文件内的偏移量，然后使用byte数组，读取一个page的内容
- iterator的实现，pageIndex改变放到hasNext中（不放到next中是因为有可能导致hasNext返回错误答案），next调用hashNext方法来决定返回tuple还是null。

#### 1.6：SeqScan
- 注意getTd时域名需要加上表的别名前缀

注意各种例外的处理，若子方法可能出现例外，那么相应的调用方法也要提供例外处理，或者在函数前面声明一下。


### Lab1
#### 1.1: Filter and Join
- 谓词比较时注意调用顺序a.equals(b)和b.equals(a)不一样
- merge注意输入为null时的处理
- open和close时需要和父类的相应方法相结合（因为是继承了父类，父类中设置了某些标志变量open和next）
- 若需要循环读取子operator，需要调用rewind方法

#### 1.2：Aggerator
- Group时使用哈希表，注意哈希表的key和value的设计，尽量简略
- 对于没有group field的情况，则只有一个组，即对整个列做aggregate，不是每条记录一个组
- tupleDesc的生成需要仔细一点，尤其是field数目和type，什么时候使用child的td以及什么时候使用this的td需要了然于心
- Iterator的设计需要仔细斟酌，可以借助arraylist实现。
- 总之每一个参数都需要认真思考！！！