# mongodb-shell
mongodb java 外键查询 数据检验
该工程的主要工作是对mongo-java-driver进行了简单的包装，对原有的对数据库集合的操作进行了扩展，
将原有的MongoDB连接封装到Connection类中；
将原来的对集合操作封装到Collection类中，并通过继承Collection类，将Foreign类聚合于ForeignCollection类中，实现外键的添加。
OperationWithUser类继承于ForeignCollection类，实现对数据的标准化操作，
其对外API的参数有操作类型（String op），输入的数据（Document input），进行操作的用户（Document user），
实现对数据的验证和权限的控制。
为了进一步方便操作，在Mongo类中，设置一个<集合名称，集合操作接口对象>的HashMap。在初始化时，通过数据库的自定义的配置文件（mongo.conf）
将集合的ID类型，外键关系等，加载进HashMap中。、
进而实现通过Mongo类，获得对某个集合的操作接口对象。
在util包中实现了一下，对org.bson.Document数据操作，json数据的获取，扁平化处理，与String类型的转化等等。
还有对文件，时间，异常，String处理的包装类。
