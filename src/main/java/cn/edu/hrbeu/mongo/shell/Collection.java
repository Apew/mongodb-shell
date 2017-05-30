package cn.edu.hrbeu.mongo.shell;

import cn.edu.hrbeu.mongo.shell.util.D;
import cn.edu.hrbeu.mongo.shell.util.Docat;
import cn.edu.hrbeu.mongo.shell.util.Documents;
import cn.edu.hrbeu.mongo.shell.util.Runtimes;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.*;

/**
 * Created by wu on 2017/5/25.
 */
public class Collection {

    static public final int COLLECTION_TYPE_AUTO_INT = 0;
    static public final int COLLECTION_TYPE_STRING = 1;
    static public final int COLLECTION_TYPE_AUTO_LONG = 2;
    static public final int COLLECTION_TYPE_MONGO_OID = 3;

    static final String _ID = "_id";
    static final Document NO_FILTERS = new Document();

    protected String collectionName;
    protected int keyType;
    protected final MongoCollection<Document> collection;
    //使用sequences集合存储集合序列号
    protected final MongoCollection<Document> sequences;

    public Collection(String collectionName, int keyType, Connection conn) {
        this.collectionName = collectionName;
        this.keyType = keyType;
        this.collection = conn.getCollection(collectionName);
        this.sequences = (keyType == COLLECTION_TYPE_AUTO_INT || keyType == COLLECTION_TYPE_AUTO_LONG) ? conn.getCollection("sequences") : null;
    }

    // 获取当前LONG的序列号
    public long getLongSequence() {
        if (keyType != COLLECTION_TYPE_AUTO_LONG) {
            return 0L;
        }
        Document ret = this.sequences.findOneAndUpdate(new Document(_ID, this.collectionName),
                new Document("$inc", new Document("seq", 1L)));
        if (ret == null) {
            this.sequences.insertOne(new Document(_ID, this.collectionName).append("seq", 1L));
            return 1L;
        } else {
            return ret.getLong("seq") + 1;
        }
    }

    // 获取当前INT的序列号
    public int getIntSequence() {
        if (keyType != COLLECTION_TYPE_AUTO_INT) {
            return 0;
        }
        Document ret = this.sequences.findOneAndUpdate(new Document(_ID, this.collectionName),
                new Document("$inc", new Document("seq", 1)));
        if (ret == null) {
            this.sequences.insertOne(new Document(_ID, this.collectionName).append("seq", 1));
            return 1;
        } else {
            return ret.getInteger("seq") + 1;
        }
    }

    public Object __ID(Object _id) {
        return this.__ID(_id, this.keyType, true);
    }

    public Object __ID(Object _id, boolean canNotMissed) {
        return this.__ID(_id, this.keyType, canNotMissed);
    }

    // 检查_id是否有效
    public Object __ID(Object _id,
                       int keyType,
                       boolean canNotMissed // 可以缺失
    ) {
        if (_id == null) {
            if (canNotMissed) {
                throw Runtimes.newException("_id 缺失");
            } else if (_id == null) {
                return null;
            }
        }
        if (keyType == COLLECTION_TYPE_MONGO_OID) {
            if (_id instanceof String) {
                String sid = ((String) _id).trim();
                if (sid.isEmpty()) {
                    throw Runtimes.newException("_id 类型 OID 格式错误1");
                } else {
                    return new ObjectId((String) sid);
                }
            } else if (_id instanceof ObjectId) {
                return _id;
            } else {
                throw Runtimes.newException("_id 类型 OID 格式错误2");
            }
        } else if (this.keyType == COLLECTION_TYPE_AUTO_LONG) {
            if (_id instanceof Integer && _id instanceof Long) {
                return (long) _id;
            } else if (_id instanceof String) {
                return Long.parseLong((String) _id);
            } else {
                throw Runtimes.newException("_id 类型 long 格式错误");
            }
        } else if (this.keyType == COLLECTION_TYPE_AUTO_INT) {
            if (_id instanceof Integer) {
                return (int) _id;
            } else if (_id instanceof String) {
                return Integer.parseInt((String) _id);
            } else {
                throw Runtimes.newException("_id 类型 int 格式错误");
            }
        } else if (_id instanceof String) {
            return _id;
        } else {
            throw Runtimes.newException("_id 类型 String 格式错误");
        }
    }

    // 用于内部访问，仅返回数据库查询结果，不返回RETURN_CODE_TAG等提示信息
    public Document __get(Document where,Document order, Document select) {
        return (Document) this.collection
                    .find(where == null ? NO_FILTERS : where)
                    .projection(select)
                    .sort(order)
                    .first();
    }

    public Document __get_by_id(Object _id, String... fields) {
        if (_id == null) {
            return null;
        }
        return this.__get(new Document(_ID, this.__ID(_id)), null, Documents.__select(fields));
    }

    // 用于内部访问，fields指定记录字段
    public Document __get_by_id(Object _id, Document select) {
        if (_id == null) {
            return null;
        }
        return this.__get(new Document(_ID, this.__ID(_id)), null, select);
    }

    // 非分页获取
    public List<Document> __list(Document where,
                                 Document order,
                                 Document select) {
        MongoCursor<Document> cursor = collection
                .find(where == null ? NO_FILTERS : where)
                .projection(select)
                .sort(order)
                .iterator();
        List<Document> list = new ArrayList();
        try {
            while (cursor.hasNext()) {
                list.add(cursor.next());
            }
        } finally {
            cursor.close();
        }
        return list;
    }

    // 分页获取
    public List<Document> __list(Document where,
                                 Document order,
                                 int skip,
                                 int limit,
                                 Document select) {
        MongoCursor<Document> cursor = collection
                .find(where == null ? NO_FILTERS : where)
                .projection(select)
                .sort(order)
                .skip(skip)
                .limit(limit)
                .iterator();
        List<Document> list = new ArrayList();
        try {
            while (cursor.hasNext()) {
                list.add(cursor.next());
            }
        } finally {
            cursor.close();
        }
        return list;
    }

    // 初始化输入的 _id
    private void __init_input_id(Object _id, Document doc) {
        switch (this.keyType) {
            case COLLECTION_TYPE_MONGO_OID:
                if (_id != null) {
                    if (_id instanceof ObjectId) {
                        doc.append(_ID, _id);
                    } else if (_id instanceof String) {
                        String sid = (String) _id;
                        doc.append(_ID, new ObjectId(sid));
                    }
                }
                break;
            case COLLECTION_TYPE_AUTO_LONG:
                doc.append(_ID, this.getLongSequence());
                break;
            case COLLECTION_TYPE_AUTO_INT:
                doc.append(_ID, this.getIntSequence());
                break;
            case COLLECTION_TYPE_STRING:
                if (_id == null) {
                    _id = new ObjectId().toString();
                } else if (!(_id instanceof String)) {
                    throw new RuntimeException("主键设定错误");
                }
                doc.append(_ID, _id);
                break;
            default:
                break;
        }
    }

    /**
     * 插入主键为_id的数据
     *
     * @param _id 主键，对于自增或OID主键，该字段无意义
     * @param values 值
     * @param createTime 创建时间
     * @return
     */
    public Document __insert_one(Object _id, Document values, long createTime) {
        this.__init_input_id(_id, values);
        if (createTime > 0) {
            values.append("createTime", createTime);
        }
        this.collection.insertOne(values);
        return values;
    }

    /*
     * 添加多条记录
     */
    public boolean __insert_many(List<Document> list, long createTime) {

        try {
            for (Document doc : list) {
                this.__init_input_id(null, doc);
                if (createTime > 0) {
                    doc.append("createTime", createTime);
                }
            }
            this.collection.insertMany(list);
            return true;
        } catch (Exception ex) {
            return false;
        }
    }

    /*
     * 记录条数
     * @param where
     */
    public long __count(Document where) {
        return collection.count(where);
    }


    /**
     * 用于内部访问，每次只能修改一组记录
     *
     * @param where
     * @param values
     * @param order
     * @param select
     * @param updateTime
     * @return
     */
    public Document __update_one(Document where,
                                 Document values,
                                 Document order,
                                 long updateTime,
                                 Document select) {
        if (updateTime > 0) {
            values.append("updateTime", updateTime);
        }
        Document setValues = new Document("$set", Documents.delayering(values));
        return collection.findOneAndUpdate(where, setValues, new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER).sort(order).projection(select));
    }

    public long __update_many(Document where, Document values, long updateTime) {
        if (updateTime > 0) {
            values.append("updateTime", updateTime);
        }
        Document setValues = new Document("$set", Documents.delayering(values));
        UpdateResult res = collection.updateMany(where, setValues);
        return res.getModifiedCount();
    }

    /**
     * 用于内部访问，每次只能修改一组记录
     *
     * @param values
     * @param select
     * @param updateTime
     * @return
     */
    public Document __update_by_id(Object id,
                                   Document values,
                                   long updateTime,
                                   Document select) {
        Document where = new Document(_ID, this.__ID(id));
        return this.__update_one(where, values, null, updateTime, select);
    }

    public Document __update_by_id(Object id,
                                   Document values,
                                   long updateTime,
                                   String... select) {
        Document where = new Document(_ID, this.__ID(id));
        return this.__update_one(where, values, null, updateTime, Documents.__select(select));
    }

    /**
     * 用于内部访问，向集合中插入document
     *
     * @param where
     * @param key
     * @param select
     * @param itemValue
     * @param updateTime
     * @return
     */
    public Document __array_insert_one(Document where, String key,
                                       Document itemValue, long updateTime, Document select) {
        // 为数据项添加ID
        itemValue.append(_ID, new ObjectId().toString());
        Document pushValues = new Document("$addToSet", new Document(key, itemValue));
        if (updateTime > 0) {
            pushValues.append("$set", new Document("updateTime", updateTime));
        }
        Document res = collection.findOneAndUpdate(where, pushValues, new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER).projection(select));
        return res;
    }

    /**
     * 用于内部访问，向集合修改document
     *
     * @param where
     * @param key
     * @param select
     * @param itemValue
     * @param updateTime
     * @return
     */
    public Document __array_update_one(Document where, String key, String _iid,
                                       Document itemValue, long updateTime, Document select) {
        // 为数据项添加ID
        where.append(key + "." + _ID, _iid);
        D.trace("数组修改条件：", where.toJson());
        Document delayeringValue = Documents.delayering(itemValue, key + ".$");
        D.trace("数组修改数值：", delayeringValue.toJson());
        if (updateTime > 0) {
            delayeringValue.append("updateTime", updateTime);
        }
        Document setValues = new Document("$set", delayeringValue);
        Document res = collection.findOneAndUpdate(where, setValues, new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER).projection(select));
        return res;
    }

    /**
     * 用于内部访问，向集合中插入document
     *
     * @param where
     * @param key
     * @param values
     * @param select
     * @param updateTime
     * @return
     */
    public Document __array_delete_many(Document where, String key,
                                        List<String> values, long updateTime, Document select) {
        Document pushValues = new Document("$pull", new Document(key, new Document(_ID, new Document("$in", values))));
        D.trace("数组删除数值：", pushValues.toJson());
        if (updateTime > 0) {
            pushValues.append("$set", new Document("updateTime", updateTime));
        }
        Document res = collection.findOneAndUpdate(where, pushValues, new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER).projection(select));
        return res;
    }

    /**
     * 用于内部访问，向集合中插入数据
     *
     * @param where
     * @param key
     * @param select
     * @param value
     * @param updateTime
     * @return
     */
    public Document __push_one(Document where, String key,
                               Object value, long updateTime, Document select) {
        Document pushValues = new Document("$addToSet", new Document(key, value));
        if (updateTime > 0) {
            pushValues.append("$set", new Document("updateTime", updateTime));
        }
        Document res = collection.findOneAndUpdate(where, pushValues, new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER).projection(select));
        return res;
    }

    /**
     * 用于内部访问，向多条记录的集合中插入数据
     *
     * @param where
     * @param key
     * @param value
     * @param updateTime
     * @return
     */
    public int __push_many(Document where, String key,
                           Object value, long updateTime) {
        Document pushValues = new Document("$addToSet", new Document(key, value));
        if (updateTime > 0) {
            pushValues.append("$set", new Document("updateTime", updateTime));
        }
        UpdateResult res = collection.updateMany(where, pushValues);
        return (int) res.getModifiedCount();
    }

    // 向数组中插入多条记录
    public Document __push_one_list(Document where,
                                    String key, List values, long updateTime, Document select) {
        Document tags = new Document(key, new Document("$each", values));
        Document pushValues = new Document("$addToSet", tags);
        Document res = collection.findOneAndUpdate(where, pushValues, new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER).projection(select));
        return res;
    }

    // 向数组中插入多条记录
    public Document __push_one_list_by_id(Object _id,
                                          String key, List values, long updateTime, Document select) {
        return this.__push_one_list(new Document(_ID, _id), key, values, updateTime, select);
    }

    public Document __push_by_id(Object id, String key, Object value, Document select) {
        Document where = new Document(_ID, id);
        return this.__push_one(where, key, value, System.currentTimeMillis(), select);
    }

    public Document __push_by_id(Object id, String key, Object value, Document values, String... fields) {
        Document select = Documents.__select(fields);
        Document where = new Document(_ID, id);
        return this.__push_one(where, key, value, System.currentTimeMillis(), select);
    }

    /**
     * 用于内部访问，从向集合中删除数据
     *
     * @param where
     * @param key
     * @param value
     * @param select
     * @param updateTime
     * @return
     */
    public Document __pull_one(Document where,
                               String key, Object value, long updateTime, Document select) {
        Document pushValues = new Document("$pull", new Document(key, value));
        if (updateTime > 0) {
            pushValues.append("$set", new Document("updateTime", updateTime));
        }
        Document res = collection.findOneAndUpdate(where, pushValues, new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER).projection(select));
        return res;
    }

    /**
     * 用于内部访问，向多条记录的集合中插入数据
     *
     * @param where
     * @param key
     * @param value
     * @param updateTime
     * @return
     */
    public int __pull_many(Document where, String key,
                           Object value, long updateTime) {
        Document pushValues = new Document("$pull", new Document(key, value));
        if (updateTime > 0) {
            pushValues.append("$set", new Document("updateTime", updateTime));
        }
        UpdateResult res = collection.updateMany(where, pushValues);
        return (int) res.getModifiedCount();
    }

    // 一次从集合中删除多项
    public Document __pull_one_list(Document where,
                                    String key, List values, long updateTime, Document select) {
        Document res = null;
        Document pushValues = new Document("$pull", new Document(key, new Document("$in", values)));
        res = collection.findOneAndUpdate(where, pushValues, new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER).projection(select));
        return res;
    }

    // 一次从集合中删除多项
    public Document __pull_one_list_by_id(Object _id,
                                          String key, List values, long updateTime, Document select) {
        return this.__pull_one_list(new Document(_ID, _id), key, values, updateTime, select);
    }

    public Document __pull_by_id(Object _id, String key, Object value, Document select) {
        Document where = new Document(_ID, _id);
        return this.__pull_one(where, key, value, System.currentTimeMillis(), select);
    }

    public Document __pull_by_id(Object _id, String key, Object value, String... fields) {
        Document select = Documents.__select(fields);
        Document where = new Document(_ID, _id);
        return this.__pull_one(where, key, value, System.currentTimeMillis(), select);
    }

    /**
     * __set 用于内部访问，设置字段值
     *
     * @param where
     * @param select
     * @param order
     * @param values
     * @param updateTime
     * @return
     */
    public Document __set_one(
            Document where,
            Document values,
            Document order,
            long updateTime, Document select) {
        if (updateTime > 0) {
            values.append("updateTime", updateTime);
        }
        FindOneAndUpdateOptions opt = new FindOneAndUpdateOptions()
                .returnDocument(ReturnDocument.AFTER)
                .sort(order).projection(select).upsert(false);
        Document setValues = new Document("$set", values);
        Document output = collection.findOneAndUpdate(where, setValues, opt);
        return output;
    }

    public long __set_many(Document where, Document values, long updateTime) {
        if (updateTime > 0) {
            values.append("updateTime", updateTime);
        }
        Document setValues = new Document("$set", values);
        UpdateResult res = collection.updateMany(where, setValues);
        return res.getModifiedCount();
    }

    public Document __set_by_id(Object _id, Document values, long updateTime, Document select) {
        return this.__set_one(new Document(_ID, _id),
                values, null, updateTime, select);
    }

    public Document __set_by_id(Object _id, Document values, long updateTime, String... fields) {
        return this.__set_one(new Document(_ID, _id),
                values, null, updateTime,
                Documents.__select(fields));
    }

    /**
     * 清除字段
     *
     * @param where
     * @param select
     * @param values
     * @param updateTime
     * @return
     */
    public Document __unset_one(Document where,
                                Document values, long updateTime, Document select) {
        Document unsetValues = new Document("$unset", values);
        if (updateTime > 0) {
            unsetValues.append("$set", new Document("updateTime", updateTime));
        }
        Document res = collection.findOneAndUpdate(where, unsetValues, new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER).projection(select));
        return res;
    }

    public long __unset_many(Document where, Document values, long updateTime) {
        Document unsetValues = new Document("$unset", values);
        if (updateTime > 0) {
            unsetValues.append("$set", new Document("updateTime", updateTime));
        }
        UpdateResult res = collection.updateMany(where, unsetValues);
        return res.getModifiedCount();
    }

    /**
     * 用于内部访问，从向集合中删除数据
     *
     * @param where
     * @return
     */
    public Document __delete_one(Document where) {
        return collection.findOneAndDelete(where, new FindOneAndDeleteOptions().projection(new Document(_ID, 1)));
    }

    /**
     * 用于内部访问，从向集合中删除数据
     *
     * @param where
     * @return
     */
    public DeleteResult __delete_many(Document where) {
        DeleteResult res = collection.deleteMany(where);
        return res;
    }

    public Document __delete_by_id(Object _id) {
        return this.__delete_one(new Document(_ID, _id));
    }

    /**
     * 通过_ID列表获取记录，并存放到Map中（命名规则遵从listByIds和getById）
     *
     * @param idList 多个_id组成的列表
     * @param select 选取字段
     * @return 保存获取结果的Map，已_id为主键
     */
    public Map __mapByIds(List idList, Document select) {
        Document where = new Document(_ID, new Document("$in", idList));
        MongoCursor<Document> cursor = collection
                .find(where)
                .projection(select)
                .iterator();
        HashMap map = new HashMap();
        try {
            while (cursor.hasNext()) {
                Document doc = cursor.next();
                map.put(doc.get(_ID), doc);
            }
        } finally {
            cursor.close();
        }
        return map;
    }

    /**
     * 通过_ID列表获取记录，并存放到Map中（命名规则遵从listByIds和getById）
     *
     * @param idList 多个_id的列表
     * @param fields 选取字段
     * @return 保存获取结果的Map，已_id为主键
     */
    public Map __mapByIds(List idList, String... fields) {
        Document select = null;
        if (fields != null && fields.length > 0) {
            select = new Document();
            for (String field : fields) {
                select.append(field, 1);
            }
        }
        return this.__mapByIds(idList, select);
    }

    // 获取指定字段 field 的不同值列表
    public List<Object> __distinct(String field, Document where, Document order) {
        List<Document> pipeline = new ArrayList<>();
        if (where != null && !where.isEmpty()) {
            pipeline.add(new Document("$match", where));
        }
        if (order != null && order.isEmpty()) {
            pipeline.add(new Document("$sort", order));
        }
        Document $group = new Document(_ID, "$" + field);
        pipeline.add(new Document("$group", $group));

        List<Object> list = new ArrayList<>();

        MongoCursor<Document> cursor = this.collection.aggregate(pipeline).iterator();
        try {
            while (cursor.hasNext()) {
                Document ti = cursor.next();
                list.add(ti.get(field));
            }
        } finally {
            cursor.close();
        }
        return list;
    }

    // 获取指定字段 field 的不同值列表，并且给出每个值出现的数量
    public List<Document> __distinct_count(String field, Document where, Document order) {
        List<Document> pipeline = new ArrayList<>();
        if (where != null && !where.isEmpty()) {
            pipeline.add(new Document("$match", where));
        }
        Document $group = new Document(_ID, "$" + field).append("count", new Document("$sum", 1));
        pipeline.add(new Document("$group", $group));
        if (order != null && !order.isEmpty()) {
            pipeline.add(new Document("$sort", order));
        }

        List<Document> list = new ArrayList<>();

        MongoCursor<Document> cursor = this.collection.aggregate(pipeline).iterator();
        try {
            while (cursor.hasNext()) {
                Document ti = cursor.next();
                ti.append("count", Docat.getInteger(ti, "count"));
                list.add(ti);
            }
        } finally {
            cursor.close();
        }
        return list;
    }

    // 获取指定字段 field 的不同值列表，并且给出每个值出现的数量，同时统计 亲近字段closeFiels 的数量
    public List<Document> __distinct_count_2(String field, Document where, Document order, String... closeFields) {
        return this.__distinct_count_2(field, where, order, closeFields == null ? null : Arrays.asList(closeFields));
    }

    // 获取指定字段 field 的不同值列表，并且给出每个值出现的数量，同时统计 亲近字段closeFiels 的数量
    public List<Document> __distinct_count_2(String field, Document where, Document order, List<String> closeFieldList) {
        List<Document> pipeline = new ArrayList<>();
        if (where != null && !where.isEmpty()) {
            pipeline.add(new Document("$match", where));
        }
        Document $group = new Document(_ID, "$" + field).append(field + "Count", new Document("$sum", 1));
        if (closeFieldList != null && closeFieldList.size() > 0) {
            for (String cf : closeFieldList) {
                $group.append(cf, new Document("$addToSet", "$" + cf));
            }
        }
//        M.trace(Cache.doc2json($group));
        pipeline.add(new Document("$group", $group));
        if (order != null && !order.isEmpty()) {
            pipeline.add(new Document("$sort", order));
        }

        Document $project = new Document(field, "$" + _ID).append(field + "Count", 1);
        if (closeFieldList != null && closeFieldList.size() > 0) {
            for (String sf : closeFieldList) {
                //{guojia:{ $size: "$guojia" }
                $project.append(sf + "Count", new Document("$size", "$" + sf));
            }
        }
        pipeline.add(new Document("$project", $project));

        List<Document> list = new ArrayList<>();
        MongoCursor<Document> cursor = this.collection.aggregate(pipeline).iterator();
        try {
            while (cursor.hasNext()) {
                Document ti = cursor.next();
                ti.append("count", Docat.getInteger(ti, "count"));
                list.add(ti);
            }
        } finally {
            cursor.close();
        }
        return list;
    }

    // 获取指定字段 field 的不同值列表，并且给出每个值出现的数量
    public List<Document> __aggregate(List<Document> pipeline) {
        List<Document> list = new ArrayList<>();
        MongoCursor<Document> cursor = this.collection.aggregate(pipeline).iterator();
        try {
            while (cursor.hasNext()) {
                Document ti = cursor.next();
                ti.append("count", Docat.getInteger(ti, "count"));
                list.add(ti);
            }
        } finally {
            cursor.close();
        }
        return list;
    }

    public List<Document> __aggregate(Document... pipes) {
        return this.__aggregate(Arrays.asList(pipes));
    }

}
