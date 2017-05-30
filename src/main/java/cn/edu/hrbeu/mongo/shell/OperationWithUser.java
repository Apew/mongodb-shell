package cn.edu.hrbeu.mongo.shell;

import cn.edu.hrbeu.mongo.shell.util.Docat;
import cn.edu.hrbeu.mongo.shell.util.Documents;
import cn.edu.hrbeu.mongo.shell.util.Runtimes;
import cn.edu.hrbeu.mongo.shell.util.Strings;
import com.mongodb.Block;
import com.mongodb.client.DistinctIterable;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

import java.util.*;

/**
 * Created by wu on 2017/5/26.
 */
public class OperationWithUser extends ForeignCollection {

    protected String itemName;
    protected String listName;
    private final Document listSelect = null;

    public OperationWithUser(String collectionName, int keyType, Connection conn, String itemName, String listName) {
        super(collectionName, keyType, conn);
        this.itemName = itemName;
        this.listName = listName;
    }

    public String getItemName() {
        return itemName;
    }

    public String getListName() {
        return listName;
    }

    public Document api(String op, Document input, Document user) {
//        M.trace("/进入API：", this.name, "_", op);
        Document output = null;

        if (op.equalsIgnoreCase("page")) {
            return this.pageItems(input, user);
        }

        if (user == null && !this.isPublic()) {
            output = new Document(Key.RETURN_CODE_TAG, "NOT_LOGINED");
        } else {
            switch (op) {
                case "insert": {
                    output = this.insertItem(input, user);
                    break;
                }
                case "get": {
                    output = this.getItem(input, user);
                    break;
                }
                case "update": {
                    output = this.updateItem(input, user);
                    break;
                }
                case "upsert": {
                    output = this.upsertItem(input, user);
                    break;
                }
                case "set": {
                    output = this.setItem(input, user);
                    break;
                }
                case "array-insert": {
                    output = this.arrayInsertItem(input, user);
                    break;
                }
                case "array-update": {
                    output = this.arrayUpdateItem(input, user);
                    break;
                }
                case "array-delete": {
                    output = this.arrayDeleteItems(input, user);
                    break;
                }
                case "vector-push": {
                    output = this.vectorPushItems(input, user);
                    break;
                }
                case "vector-pull": {
                    output = this.vectorPullItems(input, user);
                    break;
                }
                case "vector-update": {
                    output = this.vectorUpdateItem(input, user);
                    break;
                }
                case "push": {
                    output = this.pushItem(input, user);
                    break;
                }
                case "pushlist": {
                    output = this.pushList(input, user);
                    break;
                }
                case "pulllist": {
                    output = this.pullList(input, user);
                    break;
                }
                case "pull": {
                    output = this.pullItem(input, user);
                    break;
                }
                case "list": {
                    output = this.getList(input, user);
                    break;
                }
                case "delete": {
                    output = this.deleteItem(input, user);
                    break;
                }
                case "deletemany": {
                    output = this.deleteMany(input, user);
                    break;
                }
                case "options": {
                    output = this.optionItems("name", "_id");
                    break;
                }
                case "count": {
                    output = this.count(input, user);
                    break;
                }
                case "distinct-count": {
                    output = this.distinctCount(input, user);
                    break;
                }
                case "quick-search": {
                    output = this.quickSearch(input, user);
                    break;
                }
                case "distinct": {
                    output = this.distinctString(input, user);
                    break;
                }
//                case "lookup": {
//                    output = this.lookup(input, user);
//                    break;
//                }
                case "clear": {
                    output = this.clear(input, user);
                    break;
                }
                case "aggregate": {
                    output = this.aggregate(input, user);
                    break;
                }
                case "top": {
                    output = this.getTop(input, user);
                    break;
                }
            }
        }
        return output;
    }
    /**
     * 插入
     *
     * @param input {zhenyuan:{...}}
     * @param user
     * @return
     */
    public Document insertItem(Document input, Document user) {
        Document item = (Document) input.get(this.itemName);
        if (item == null) {
            return new Document(Key.RETURN_CODE_TAG, "输入格式错误，no item value");
        }
        if (this.isHasParent()) {
            if (!input.containsKey(Key._PID)) {
                return new Document(Key.RETURN_CODE_TAG, "输入格式错误，no _pid");
            }
            int _pid = input.getInteger(Key._PID, 0);
            if (_pid < 0) {
                return new Document(Key.RETURN_CODE_TAG, "输入格式错误，_pid<0");
            }
            item.append(Key._PID, _pid);
        } else if (input.containsKey(Key._PID) || item.containsValue(Key._PID)) {
            return new Document(Key.RETURN_CODE_TAG, "输入格式错误，_pid不该出现");
        }

        if (this.isHasFarther()) {
            if (!input.containsKey(Key._FID)) {
                return new Document(Key.RETURN_CODE_TAG, "输入格式错误，no _fid");
            }
            int _fid = input.getInteger(Key._FID, 0);
            if (_fid < 0) {
                return new Document(Key.RETURN_CODE_TAG, "输入格式错误，_fid<0");
            }
            item.append(Key._FID, _fid);
        } else if (input.containsKey(Key._FID) || item.containsValue(Key._FID)) {
            return new Document(Key.RETURN_CODE_TAG, "输入格式错误，_fid不该出现");
        }
        if (item.containsKey(_ID)) {
            return new Document(Key.RETURN_CODE_TAG, "输入格式错误");
        }
        Object _id = null;
        if (this.keyType == Collection.COLLECTION_TYPE_STRING) {
            _id = input.get(_ID);
        }
        if (user == null) {
            return new Document(Key.RETURN_CODE_TAG, "必须是登陆用户");
        }
        Object _uid = user.get(_ID);
        item.append(Key._USER, _uid);
        item.append("status", 1);
        Document output = this.__insert_one(_id, item, System.currentTimeMillis());
        if (output != null) {
            this.appendAllForeignKey(output);
            this.fire("insert", output);
            return new Document(Key.RETURN_CODE_TAG, "OK").append(this.itemName, output);
        } else {
            return new Document(Key.RETURN_CODE_TAG, "添加失败");
        }

    }

    /**
     * 删除
     *
     * @param input {value:{_id:1}}
     * @param user
     * @return
     */
    public Document deleteItem(Document input, Document user) {
        //查询基本数据
        Object _id = this.__ID(input.get(_ID));

        Document where = new Document(_ID, _id);
        Object _uid = null;
        if (user != null) {
            _uid = user.get(_ID);
            if (user.getInteger("role", 0) == 9) {
                // 可以访问
            } else {
                // 如果权限未通过的话就判断记录是否属于该用户
                where.append(Key._USER, _uid);
            }
        }
        Document item = this.__delete_one(where);
        if (item != null) {
            return new Document(Key.RETURN_CODE_TAG, "OK").append(this.itemName, item);
        } else {
            return new Document(Key.RETURN_CODE_TAG, "删除失败");
        }
    }

    public Document deleteMany(Document input, Document user) {
        //查询基本数据
        List ids = input.get("_ids", List.class);
        if (ids == null || ids.isEmpty()) {
            return new Document(Key.RETURN_CODE_TAG, "输入格式错误，ids is null or empty");
        }
        for (int i = 0; i < ids.size(); i++) {
            ids.set(i, this.__ID(ids.get(i)));
        }
        Document where = new Document(_ID, new Document("$in", ids));

        Object _uid = null;
        if (user != null) {
            _uid = user.get(_ID);
            if (user.getInteger("role", 0) == 9) {
                // 可以访问
            } else {
                // 如果权限未通过的话就判断记录是否属于该用户
                where.append(Key._USER, _uid);
            }
        }
        this.__delete_many(where);
        return new Document(Key.RETURN_CODE_TAG, "OK");
    }

    public Document clear(Document input, Document user) {
        Document where = Docat.getDocument(input, "where", new Document());
        Object _uid = null;
        if (user != null) {
            _uid = user.get(_ID);
            if (user.getInteger("role", 0) == 9) {
                // 可以访问
            } else {
                // 如果权限未通过的话就判断记录是否属于该用户
                where.append(Key._USER, _uid);
            }
        }
        this.__delete_many(where);
        return new Document(Key.RETURN_CODE_TAG, "OK");
    }

    /**
     * 获取
     *
     * @param input
     * {where:{_id:number},select:{[tarInfo,basStru,shipSupSys,serveInfo,charInfo,addInfo]:1},order:{}}
     * @param user
     * @return
     */
    public Document getItem(Document input, Document user) {
        //查询基本数据
        Object _id = __ID(input.get(_ID));

        Document where = new Document(_ID, _id);
        Document select = (Document) input.get("select");

        if (this.isPublic()) {
            // public 读取不做任何限制
        } else if (user != null) {
            if (user.getInteger("role", 0) >= 8) {
                // 可以访问
            } else {
                // 如果权限未通过的话就判断记录是否属于该用户
                where.append(Key._USER, user.get(_ID));
            }
        }
        Document item = this.__get(where, null, select);
        if (item != null) {
            this.appendAllForeignKey(item);
            return new Document(Key.RETURN_CODE_TAG, "OK").append(this.itemName, item);
        } else {
            return new Document(Key.RETURN_CODE_TAG, "未找到记录");
        }
    }

    /**
     * 获取
     *
     * @param input
     * {where:{_id:number},select:{[tarInfo,basStru,shipSupSys,serveInfo,charInfo,addInfo]:1},order:{}}
     * @param user
     * @return
     */
    public Document getTop(Document input, Document user) {
        //查询基本数据

        Document where = Docat.getDocument(input, "where", new Document());
        Document select = (Document) input.get("select");
        Document order = Docat.getDocument(input, "order");

        if (this.isPublic()) {
            // public 读取不做任何限制
        } else if (user != null) {
            if (user.getInteger("role", 0) >= 8) {
                // 可以访问
            } else {
                // 如果权限未通过的话就判断记录是否属于该用户
                where.append(Key._USER, user.get(_ID));
            }
        }
        Document item = this.__get(where, order, select);
        if (item != null) {
            this.appendAllForeignKey(item);
            return new Document(Key.RETURN_CODE_TAG, "OK").append(this.itemName, item);
        } else {
            return new Document(Key.RETURN_CODE_TAG, "未找到记录");
        }
    }

    /**
     * 修改
     *
     * @param input {values:{ name:'admin'}, select:{_id:0}}, where:{...}}"
     * values 修改的数据 select 返回的字段 where 条件
     * @param user
     * @return
     */
    public Document updateItem(Document input, Document user) {
        Object _id = __ID(input.get(_ID));

        Document value = (Document) input.get(this.collectionName);
        if (value == null || value.containsKey(_ID)) {
            return new Document(Key.RETURN_CODE_TAG, "输入格式错误 value has id");
        }
        if (this.isHasParent()) {
            if (value.containsKey(Key._PID)) {
                return new Document(Key.RETURN_CODE_TAG, "输入格式错误 value has pid");
            }
        }
        if (this.isHasFarther()) {
            if (value.containsKey(Key._FID)) {
                return new Document(Key.RETURN_CODE_TAG, "输入格式错误 value has fid");
            }
        }

        Document where = new Document(_ID, _id);
        Object _uid = null;
        if (user != null) {
            _uid = user.get(_ID);
            if (user.getInteger("role", 0) >= 9) {
                // 可以访问
            } else {
                // 如果权限未通过的话就判断记录是否属于该用户
                where.append(Key._USER, _uid);
            }
        }

        Document select = (Document) input.get("select");
        Document item = this.__update_one(where, value, null, System.currentTimeMillis(), select);
        if (item != null) {
            this.appendAllForeignKey(item);
            return new Document(Key.RETURN_CODE_TAG, "OK").append(this.itemName, item);
        } else {
            return new Document(Key.RETURN_CODE_TAG, "指定记录不存在");
        }
    }

    /**
     * 修改
     *
     * @param input {values:{ name:'admin'}, select:{_id:0}}, where:{...}}"
     * values 修改或添加的数据 select 返回的字段 where 条件
     * @param user
     * @return
     */
    public Document upsertItem(Document input, Document user) {
        Object _id = __ID(input.get(_ID));

        Document value = (Document) input.get(this.itemName);
        if (value == null || value.containsKey(_ID)) {
            return new Document(Key.RETURN_CODE_TAG, "输入格式错误 value has id");
        }
        if (this.isHasParent()) {
            if (value.containsKey(Key._PID)) {
                return new Document(Key.RETURN_CODE_TAG, "输入格式错误 value has pid");
            }
        }
        if (this.isHasFarther()) {
            if (value.containsKey(Key._FID)) {
                return new Document(Key.RETURN_CODE_TAG, "输入格式错误 value has fid");
            }
        }

        Document select = (Document) input.get("select");
        Document existItem = this.__get_by_id(_id, _ID,Key._USER);
        if (existItem != null) { // 有记录
            Object _uid = null;
            if (user != null) {
                int role = user.getInteger("role", 0);
                _uid = user.get(_ID);
                if (role < 9 && !_uid.equals(existItem.get(Key._USER))) {
                    return new Document(Key.RETURN_CODE_TAG, "无权限，不能修改");
                }
            }
            Document item = this.__update_by_id(_id, value, System.currentTimeMillis(), select);
            if (item != null) {
                this.appendAllForeignKey(item);
                return new Document(Key.RETURN_CODE_TAG, "OK").append(this.itemName, item);
            } else {
                return new Document(Key.RETURN_CODE_TAG, "指定记录不存在");
            }
        } else { // 不存在，添加
            Object _uid = user == null ? null : user.get(_ID);
            if (_uid != null) {
                value.append(Key._USER, _uid);
            }
            value.append("status", 1);
            Document output = this.__insert_one(_id, value, System.currentTimeMillis());
            if (output != null) {
                this.appendAllForeignKey(output);
                return new Document(Key.RETURN_CODE_TAG, "OK").append(this.itemName, output);
            } else {
                return new Document(Key.RETURN_CODE_TAG, "添加失败");
            }
        }
    }

    /**
     * 数组中天加一个Document
     *
     * @param input {values:{ name:'admin'}, select:{_id:0}}, where:{...}}"
     * values 修改的数据 select 返回的字段 where 条件
     * @param user
     * @return
     */
    public Document arrayInsertItem(Document input, Document user) {
        Object _id = __ID(input.get(_ID));
        String key = input.getString("key");
        if (Strings.isEmptyStrictly(key)) {
            return new Document(Key.RETURN_CODE_TAG, "输入格式错误 key missed");
        }
        Document value = input.get(this.itemName, Document.class);
        if (value == null) {
            return new Document(Key.RETURN_CODE_TAG, "输入格式错误 value missed");
        }
        Document select = (Document) input.get("select");
        Document where = new Document(_ID, _id);
        Object _uid = null;
        if (user != null) {
            _uid = user.get(_ID);
            if (user.getInteger("role", 0) >= 8) {
                // 可以访问
            } else {
                // 如果权限未通过的话就判断记录是否属于该用户
                where.append(Key._USER, _uid);
            }
        }
        Document item = this.__array_insert_one(where, key, value, System.currentTimeMillis(), select);
        if (item != null) {
            this.appendAllForeignKey(item);
            return new Document(Key.RETURN_CODE_TAG, "OK").append(this.itemName, item);
        } else {
            return new Document(Key.RETURN_CODE_TAG, "push失败");
        }
    }

    /**
     * 数组中天加一个Document
     *
     * @param input {values:{ name:'admin'}, select:{_id:0}}, where:{...}}"
     * values 修改的数据 select 返回的字段 where 条件
     * @param user
     * @return
     */
    public Document arrayUpdateItem(Document input, Document user) {
        Object _id = __ID(input.get(_ID));
        String key = input.getString("key");
        if (Strings.isEmptyStrictly(key)) {
            return new Document(Key.RETURN_CODE_TAG, "输入格式错误 key missed");
        }
        Document value = input.get(this.itemName, Document.class);
        if (value == null) {
            return new Document(Key.RETURN_CODE_TAG, "输入格式错误 value missed");
        }
        // 子项id
        String _iid = value.getString(_ID);
        if (Strings.isEmptyStrictly(_iid)) {
            return new Document(Key.RETURN_CODE_TAG, "输入格式错误 value's id missed");
        }
        value.remove(_ID);

        Document select = (Document) input.get("select");
        Document where = new Document(_ID, _id);
        Object _uid = null;
        if (user != null) {
            _uid = user.get(_ID);
            if (user.getInteger("role", 0) >= 8) {
                // 可以访问
            } else {
                // 如果权限未通过的话就判断记录是否属于该用户
                where.append(Key._USER, _uid);
            }
        }
        Document item = this.__array_update_one(where, key, _iid, value, System.currentTimeMillis(), select);
        if (item != null) {
            this.appendAllForeignKey(item);
            return new Document(Key.RETURN_CODE_TAG, "OK").append(this.itemName, item);
        } else {
            return new Document(Key.RETURN_CODE_TAG, "push失败");
        }
    }

    /**
     * 修改
     *
     * @param input {values:{ name:'admin'}, select:{_id:0}}, where:{...}}"
     * values 修改的数据 select 返回的字段 where 条件
     * @param user
     * @return
     */
    public Document arrayDeleteItems(Document input, Document user) {
        Object _id = __ID(input.get(_ID));
        String key = input.getString("key");
        if (Strings.isEmptyStrictly(key)) {
            return new Document(Key.RETURN_CODE_TAG, "输入格式错误 field missed");
        }
        List<String> values = input.get(this.listName, List.class);
        if (values == null || values.isEmpty()) {
            return new Document(Key.RETURN_CODE_TAG, "输入格式错误 ids missed");
        }
        Document select = (Document) input.get("select");
        Document where = new Document(_ID, _id);

        Object _uid = null;
        if (user != null) {
            _uid = user.get(_ID);
            if (user.getInteger("role", 0) >= 8) {
                // 可以访问
            } else {
                // 如果权限未通过的话就判断记录是否属于该用户
                where.append(Key._USER, _uid);
            }
        }
        Document item = this.__array_delete_many(where, key, values, System.currentTimeMillis(), select);
        if (item != null) {
            this.appendAllForeignKey(item);
            this.fire("update", item);
            return new Document(Key.RETURN_CODE_TAG, "OK").append(this.itemName, item);
        } else {
            return new Document(Key.RETURN_CODE_TAG, "pull失败");
        }
    }

    /**
     * 修改
     *
     * @param input {values:{ name:'admin'}, select:{_id:0}}, where:{...}}"
     * values 修改的数据 select 返回的字段 where 条件
     * @param user
     * @return
     */
    public Document pushItem(Document input, Document user) {
        Object _id = __ID(input.get(_ID));
        String key = input.getString("key");
        if (Strings.isEmptyStrictly(key)) {
            return new Document(Key.RETURN_CODE_TAG, "输入格式错误 key missed");
        }
        Object value = input.get(this.itemName);
        if (value == null) {
            return new Document(Key.RETURN_CODE_TAG, "输入格式错误 value missed");
        }
        Document select = (Document) input.get("select");
        Document where = new Document(_ID, _id);
        Object _uid = null;
        if (user != null) {
            _uid = user.get(_ID);
            if (user.getInteger("role", 0) >= 8) {
                // 可以访问
            } else {
                // 如果权限未通过的话就判断记录是否属于该用户
                where.append(Key._USER, _uid);
            }
        }
        Document item = this.__push_one(where, key, value, System.currentTimeMillis(), select);
        if (item != null) {
            this.appendAllForeignKey(item);
            return new Document(Key.RETURN_CODE_TAG, "OK").append(this.itemName, item);
        } else {
            return new Document(Key.RETURN_CODE_TAG, "push失败");
        }
    }

    public Document pushList(Document input, Document user) {
        Object _id = __ID(input.get(_ID));
        String key = input.getString("key");
        if (Strings.isEmptyStrictly(key)) {
            return new Document(Key.RETURN_CODE_TAG, "输入格式错误 field missed");
        }
        List values = input.get(this.listName, List.class);
        if (values == null || values.isEmpty()) {
            return new Document(Key.RETURN_CODE_TAG, "输入格式错误 values is not list or empty");
        }
        Document select = (Document) input.get("select");
        Document where = new Document(_ID, _id);
        Object _uid = null;
        if (user != null) {
            _uid = user.get(_ID);
            if (user.getInteger("role", 0) >= 8) {
                // 可以访问
            } else {
                // 如果权限未通过的话就判断记录是否属于该用户
                where.append(Key._USER, _uid);
            }
        }
        Document item = this.__push_one_list(where, key, values, System.currentTimeMillis(), select);
        if (item != null) {
            this.appendAllForeignKey(item);
            return new Document(Key.RETURN_CODE_TAG, "OK").append(this.itemName, item);
        } else {
            return new Document(Key.RETURN_CODE_TAG, "push失败");
        }
    }

    /**
     * 修改
     *
     * @param input {values:{ name:'admin'}, select:{_id:0}}, where:{...}}"
     * values 修改的数据 select 返回的字段 where 条件
     * @param user
     * @return
     */
    public Document pullItem(Document input, Document user) {
        Object _id = __ID(input.get(_ID));
        String key = input.getString("key");
        if (Strings.isEmptyStrictly(key)) {
            return new Document(Key.RETURN_CODE_TAG, "输入格式错误 field missed");
        }
        Object value = input.get(this.itemName);
        if (value == null) {
            return new Document(Key.RETURN_CODE_TAG, "输入格式错误 value missed");
        }

        Document select = (Document) input.get("select");
        Document where = new Document(_ID, _id);
        Object _uid = null;
        if (user != null) {
            _uid = user.get(_ID);
            if (user.getInteger("role", 0) >= 8) {
                // 可以访问
            } else {
                // 如果权限未通过的话就判断记录是否属于该用户
                where.append(Key._USER, _uid);
            }
        }
        Document item = this.__pull_one(where, key, value, System.currentTimeMillis(), select);
        if (item != null) {
            this.appendAllForeignKey(item);

            this.fire("update", item);
            return new Document(Key.RETURN_CODE_TAG, "OK").append(this.itemName, item);
        } else {
            return new Document(Key.RETURN_CODE_TAG, "pull失败");
        }
    }

    public Document pullList(Document input, Document user) {
        Object _id = __ID(input.get(_ID));
        String key = input.getString("key");
        if (Strings.isEmptyStrictly(key)) {
            return new Document(Key.RETURN_CODE_TAG, "输入格式错误 key missed");
        }
        List values = input.get(this.listName, List.class);
        if (values == null || values.isEmpty()) {
            return new Document(Key.RETURN_CODE_TAG, "输入格式错误 values is not list or empty");
        }
        Document select = (Document) input.get("select");
        Document where = new Document(_ID, _id);
        Object _uid = null;
        if (user != null) {
            _uid = user.get(_ID);
            if (user.getInteger("role", 0) >= 8) {
                // 可以访问
            } else {
                // 如果权限未通过的话就判断记录是否属于该用户
                where.append(Key._USER, _uid);
            }
        }
        Document item = this.__pull_one_list(where, key, values, System.currentTimeMillis(), select);
        if (item != null) {
            this.appendAllForeignKey(item);

            this.fire("update", item);
            return new Document(Key.RETURN_CODE_TAG, "OK").append(this.itemName, item);
        } else {
            return new Document(Key.RETURN_CODE_TAG, "push失败");
        }
    }

    public Document vectorPushItems(Document input, Document user) {
        Object _id = __ID(input.get(_ID));
        String key = input.getString("key");
        if (Strings.isEmptyStrictly(key)) {
            return new Document(Key.RETURN_CODE_TAG, "输入格式错误 key missed");
        }
        List values = input.get(this.listName, List.class);
        if (values == null || values.isEmpty()) {
            return new Document(Key.RETURN_CODE_TAG, "输入格式错误 value missed");
        }
        Document select = Docat.getDocument(input, "select");
        Document where = new Document(_ID, _id);
        Object _uid = null;
        if (user != null) {
            _uid = user.get(_ID);
            if (user.getInteger("role", 0) >= 8) {
                // 可以访问
            } else {
                // 如果权限未通过的话就判断记录是否属于该用户
                where.append(Key._USER, _uid);
            }
        }
        Document item = this.__get(where, null, Documents.__select(key));
        if (item == null) {
            return new Document(Key.RETURN_CODE_TAG, "记录不存在");
        }
        List theList = Docat.getList(item, key, new ArrayList());
        theList.addAll(values);
        item = this.__update_by_id(_id, new Document(key, theList), System.currentTimeMillis(), select);
        if (item != null) {
            this.appendAllForeignKey(item);
            return new Document(Key.RETURN_CODE_TAG, "OK").append(this.itemName, item);
        } else {
            return new Document(Key.RETURN_CODE_TAG, "添加失败");
        }
    }

    public Document vectorPullItems(Document input, Document user) {
        Object _id = __ID(input.get(_ID));
        String key = input.getString("key");
        if (Strings.isEmptyStrictly(key)) {
            return new Document(Key.RETURN_CODE_TAG, "输入格式错误 key missed");
        }
        List<Integer> indexes = input.get("indexes", List.class);
        if (indexes == null || indexes.isEmpty()) {
            return new Document(Key.RETURN_CODE_TAG, "输入格式错误 indexes missed");
        }
        Document select = Docat.getDocument(input, "select");
        Document where = new Document(_ID, _id);
        Object _uid = null;
        if (user != null) {
            _uid = user.get(_ID);
            if (user.getInteger("role", 0) >= 8) {
                // 可以访问
            } else {
                // 如果权限未通过的话就判断记录是否属于该用户
                where.append(Key._USER, _uid);
            }
        }
        Document item = this.__get(where, null, Documents.__select(key));

        if (item == null) {
            return new Document(Key.RETURN_CODE_TAG, "记录不存在");
        }
        List theList = Docat.getList(item, key, new ArrayList());
        List newList = new ArrayList();
        Set<Integer> set = new HashSet<>();
        set.addAll(indexes);
        for (int i = 0; i < theList.size(); i++) {
            if (!set.contains(i)) {
                newList.add(theList.get(i));
            }
        }
        item = this.__update_by_id(_id, new Document(key, newList), System.currentTimeMillis(), select);
        if (item != null) {
            this.appendAllForeignKey(item);
            return new Document(Key.RETURN_CODE_TAG, "OK").append(this.itemName, item);
        } else {
            return new Document(Key.RETURN_CODE_TAG, "添加失败");
        }
    }

    public Document vectorUpdateItem(Document input, Document user) {
        Object _id = __ID(input.get(_ID));
        String key = input.getString("key");
        if (Strings.isEmptyStrictly(key)) {
            return new Document(Key.RETURN_CODE_TAG, "输入格式错误 key missed");
        }
        int index = Docat.getInteger(input, "index", -1);
        if (index < 0) {
            return new Document(Key.RETURN_CODE_TAG, "输入格式错误 index missed");
        }
        Object value = input.get(this.itemName);
        if (value == null) {
            return new Document(Key.RETURN_CODE_TAG, "输入格式错误 value missed");
        }

        Document select = Docat.getDocument(input, "select");
        Document where = new Document(_ID, _id);
        Object _uid = null;
        if (user != null) {
            _uid = user.get(_ID);
            if (user.getInteger("role", 0) >= 8) {
                // 可以访问
            } else {
                // 如果权限未通过的话就判断记录是否属于该用户
                where.append(Key._USER, _uid);
            }
        }
        Document item = this.__get(where, null, Documents.__select(key));

        if (item == null) {
            return new Document(Key.RETURN_CODE_TAG, "记录不存在");
        }
        List theList = Docat.getList(item, key, new ArrayList());
        if (index >= theList.size()) {
            return new Document(Key.RETURN_CODE_TAG, "index 记录不存在");
        }
        Object o = theList.get(index);
        if (o instanceof Document) {
            if (value instanceof Document) {
                Document doc = (Document) o;
                doc.putAll((Document) value);
                theList.set(index, doc);
            } else {
                return new Document(Key.RETURN_CODE_TAG, "数据类型不是document");
            }
        } else if (value instanceof Document) {
            return new Document(Key.RETURN_CODE_TAG, "数据类型不是常规数值");
        } else {
            theList.set(index, value);
        }
        item = this.__update_by_id(_id, new Document(key, theList), System.currentTimeMillis(), select);
        if (item != null) {
            this.appendAllForeignKey(item);
            return new Document(Key.RETURN_CODE_TAG, "OK").append(this.itemName, item);
        } else {
            return new Document(Key.RETURN_CODE_TAG, "修改失败");
        }
    }

    /**
     * 修改
     *
     * @param input {values:{ name:'admin'}, select:{_id:0}}, where:{...}}"
     * values 修改的数据 select 返回的字段 where 条件
     * @param user
     * @return
     */
    public Document setItem(Document input, Document user) {
        Object _id = __ID(input.get(_ID));
        Document values = (Document) input.get(this.itemName);
        if (values == null || values.containsKey(_ID)) {
            return new Document(Key.RETURN_CODE_TAG, "输入格式错误value has id");
        }
        if (this.isHasParent()) {
            if (values.containsKey(Key._PID)) {
                return new Document(Key.RETURN_CODE_TAG, "输入格式错误 value ha pid");
            }
        }
        if (this.isHasFarther()) {
            if (values.containsKey(Key._FID)) {
                return new Document(Key.RETURN_CODE_TAG, "输入格式错误value has fid");
            }
        }
        Document select = (Document) input.get("select");
        Document where = new Document(_ID, _id);
        Object _uid = null;
        if (user != null) {
            _uid = user.get(_ID);
            if (user.getInteger("role", 0) >= 8) {
                // 可以访问
            } else {
                // 如果权限未通过的话就判断记录是否属于该用户
                where.append(Key._USER, _uid);
            }
        }
        Document item = this.__set_one(where, values, null, System.currentTimeMillis(), select);
        if (item != null) {
            this.appendAllForeignKey(item);

            this.fire("update", item);
            return new Document(Key.RETURN_CODE_TAG, "OK").append(this.itemName, item);
        } else {
            return new Document(Key.RETURN_CODE_TAG, "set失败");
        }
    }

    public List<Document> list(Document where, Document order, String... fields) {
        return this.list(where, order, Documents.__select(fields));
    }

    // 根据条件查找列表
    public List<Document> list(Document where, Document order, Document select) {
        List<Document> list = this.__list(where, order, select);
        if (list != null && !list.isEmpty()) {
            this.appendAllForeignKey(list);
        }
        return list;
    }

    public Document createFilter(Document where, Document search, String searchString, Document user) {
        Document filter = new Document();

        if (this.isPublic()) {
            // 什么也不做
        } else if (user != null) {
            if (Docat.getInteger(user, "role", 0) >= 8) {
                // role >= 8 的用户可访问全部数据
            } else {
                // 如果权限未通过的话就判断记录是否属于该用户
                filter.append(Key._USER, user.get(_ID));
            }
        }

        if (search != null) {
            if (where == null) {
                filter.putAll(search);
            } else {
                List whereList = new ArrayList();
                whereList.add(where);
                whereList.add(search);
                filter.append("$and", whereList);
            }
        } else {
            if (where != null) {
                filter.putAll(where);
            }
            if (!Strings.isEmptyStrictly(searchString)) {
                List whereList = new ArrayList();
                for (String searchKey : this.defaultSearchFields) {
                    whereList.add(new Document(searchKey, new Document("$regex", searchString)));
                }
                filter.append("$or", whereList);
            }
        }
        return filter;
    }

    /**
     * 列表
     *
     * @param input where select order pageSize pageIndex 注：模糊查询 例
     * select:{name:{$regex:'aa'}}
     * @param user
     * @return
     */
    public Document getList(Document input, Document user) {
        Document where = input.get("where", Document.class);
        Document order = input.get("order", Document.class);
        Document select = Docat.getDocument(input, "select", new Document());
        { // 位置不合理，暂时先放在这里
            if (this.isHasParent()) {
                Object _pid = input.get(Key._PID);
                if (_pid == null) {
                    return new Document(Key.RETURN_CODE_TAG, "输入格式错误，pid 未输入");
                }
                where.append(Key._PID, _pid);
            }
            if (this.isHasFarther()) {
                Object _fid = input.get(Key._FID);
                if (_fid == null) {
                    return new Document(Key.RETURN_CODE_TAG, "输入格式错误，fid 未输入");
                }
                where.append(Key._FID, _fid);
            }
        }
        if (this.isPublic()) {
            // public 读取不做任何限制
        } else if (user != null) {
            if (user.getInteger("role", 0) >= 8) {
                // role >= 8 的用户可访问全部数据
            } else {
                if (where == null) {
                    where = new Document();
                }
                // 如果权限未通过的话就判断记录是否属于该用户
                where.append(Key._USER, user.get(_ID));
            }
        }
        int pageIndex = input.getInteger("pageIndex", 1);
        int pageSize = input.getInteger("pageSize", 20);
        if (pageIndex <= 0) {
            pageIndex = 1;
        }
        if (pageSize <= 0) {
            pageSize = 20;
        } else if (pageSize > 200) {
            pageSize = 200;
        }

        Document search = input.get("search", Document.class);
        if (search != null) {
            if (where == null) {
                where = search;
            } else {
                List whereList = new ArrayList();
                whereList.add(where);
                whereList.add(search);
                where = new Document("$and", whereList);
            }
        } else {
            String searchString = input.getString("searchString");
            if (!Strings.isEmptyStrictly(searchString)) {
                if (where == null) {
                    where = new Document();
                }
                List whereList = new ArrayList();
                for (String searchKey : this.defaultSearchFields) {
                    whereList.add(new Document(searchKey, new Document("$regex", searchString)));
                }
                where.append("$or", whereList);
            }
        }

        if (this.listSelect != null) {
            select.putAll(this.listSelect);
        }
        Document output = this.__listPage(where, select,
                order,
                pageIndex, pageSize, Key._WITH_TOTAL // 返回记录条数
        ).append(Key.RETURN_CODE_TAG, Key.OK);

        List list = (List) output.get(this.listName);
        this.appendAllForeignKey(list);
        return output;
    }

    public Document distinctString(Document input, Document user) {
        String field = input.getString("field");

        if (!Strings.isCompactedWorld(field)) {
            return new Document(Key.RETURN_CODE_TAG, "输入格式错误，缺少field");
        }

        Document where = (Document) input.get("where");
        List<String> list = new ArrayList<String>();

        DistinctIterable<String> it = this.collection.distinct(field, where, String.class);
        try {
            for (String o : it) {
                if (o != null && !o.isEmpty()) {
                    list.add(o);
                }
            }
        } catch (Exception ex) {
//            ex.printStackTrace();
        }
        return new Document(Key.RETURN_CODE_TAG, Key.OK).append("values", list);
    }

    public Document distincts(Document input, Object _uid) {
        List<String> fields = input.get("fields", List.class);

//        Document where = (Document) input.get("where");
        List<Document> pipeline = new ArrayList<>();
        Document $_id = new Document();
        Document $group = new Document(_ID, $_id);
        for (String field : fields) {
            $_id.append(field, "$" + field);
//            $group.append(field, new Document("$push", "$" + field));
        }
        pipeline.add(new Document("$group", $group));

        //this.collection.aggregate(pipeline);
        return new Document(Key.RETURN_CODE_TAG, Key.OK).append("values", this.collection.aggregate(pipeline));
    }

    public Document count(Document input, Document user) {
        Document where = (Document) input.get("where");
        { // 位置不合理，暂时先放在这里
            if (this.isHasFarther()) {
                Object _pid = input.get(Key._PID);
                if (_pid == null) {
                    return new Document(Key.RETURN_CODE_TAG, "输入格式错误，pid 未输入");
                }
                where.append(Key._PID, _pid);
            }
            if (this.isHasFarther()) {
                Object _fid = input.get(Key._FID);
                if (_fid == null) {
                    return new Document(Key.RETURN_CODE_TAG, "输入格式错误，fid 未输入");
                }
                where.append(Key._FID, _fid);
            }
        }
        if (this.isPublic()) {
            // public 读取不做任何限制
        } else if (user != null) {
            if (user.getInteger("role", 0) >= 8) {
                // role >= 8 的用户可访问全部数据
            } else {
                // 如果权限未通过的话就判断记录是否属于该用户
                where.append(Key._USER, user.get(_ID));
            }
        }
        Document search = input.get("search", Document.class);
        if (search != null) {
            if (where == null) {
                where = search;
            } else {
                List whereList = new ArrayList();
                whereList.add(where);
                whereList.add(search);
                where.append("$and", whereList);
            }
        } else {
            String searchString = input.getString("searchString");
            if (!Strings.isEmptyStrictly(searchString)) {
                if (where == null) {
                    where = new Document();
                }
                List whereList = new ArrayList();
                for (String searchKey : this.defaultSearchFields) {
                    whereList.add(new Document(searchKey, new Document("$regex", searchString)));
                }
                where.append("$or", whereList);
            }
        }
        int total = (int) collection.count(where);
        Document output = new Document(Key.RETURN_CODE_TAG, Key.OK).append("count", total);
        return output;
    }

    /**
     * 列表
     *
     * 获得某 page 数据，没有权限控制
     *
     * @param input
     * @param user
     * @return
     */
    public Document pageItems(Document input, Document user) {
        Document where = input.get("where", Document.class);

        int pageIndex = input.getInteger("pageIndex", 1);
        int pageSize = input.getInteger("pageSize", 20);
        if (pageSize > 20) {
            pageSize = 20;
        }
        Document search = input.get("search", Document.class);
        if (search != null) {
            if (where == null) {
                where = search;
            } else {
                List whereList = new ArrayList();
                whereList.add(where);
                whereList.add(search);
                where.append("$and", whereList);
            }
        } else {
            String searchString = input.getString("searchString");
            if (!Strings.isEmptyStrictly(searchString)) {
                if (where == null) {
                    where = new Document();
                }
                List whereList = new ArrayList();
                for (String searchKey : this.defaultSearchFields) {
                    whereList.add(new Document(searchKey, new Document("$regex", searchString)));
                }
                where.append("$or", whereList);
            }
        }

        Document output = this.__listPage(where, this.listSelect,
                new Document("_id", -1), // 排序
                pageIndex, pageSize, Key._WITH_TOTAL // 返回记录条数
        ).append(Key.RETURN_CODE_TAG, Key.OK);

        List list = (List) output.get(this.listName);
        this.appendAllForeignKey(list);

        return output;
    }

    /**
     * 列表
     *
     * 获得某 page 数据，没有权限控制
     *
     * @param input
     * @param user
     * @return
     */
    public Document topItem(Document input, Document user) {
        Document where = (Document) input.get("where");

        String searchString = input.getString("searchString");
        if (!Strings.isEmptyStrictly(searchString)) {
            if (where == null) {
                where = new Document();
            }
            List whereList = new ArrayList();
            for (String searchKey : this.defaultSearchFields) {
                whereList.add(new Document(searchKey, new Document("$regex", searchString)));
            }
            where.append("$or", whereList);
        }
        Document order = (Document) input.get("order");

        Document item = this.__get(where, order, null);
        Document output = new Document(Key.RETURN_CODE_TAG, item == null ? "没有符合数据" : Key.OK);
        if (item != null) {
            this.appendAllForeignKey(item);
            output.append(this.itemName, item);
        }
        if (!Strings.isEmptyStrictly(searchString)) {
            // 暂时这样解决
            output.append("searchString", searchString);
        }
        return output;
    }

    public Document optionItems(String... fields) {
        List<Document> list = this.__list(null, null, Documents.__select(fields));
        List optionList = new ArrayList();
        List option = new ArrayList();
        option.add("未知");
        if (this.keyType == COLLECTION_TYPE_STRING || this.keyType == COLLECTION_TYPE_MONGO_OID) {
            option.add(null);
        } else {
            option.add(0);
        }
        optionList.add(option);
        for (Object o : list) {
            Document obj = (Document) o;
            option = new ArrayList();
            for (String field : fields) {
                option.add(obj.get(field));
            }
            optionList.add(option);
        }
        return new Document(Key.RETURN_CODE_TAG, "OK").append(this.listName, optionList);
    }

    // 分页获取
    public Document __listPage(Document where,
                               Document select,
                               Document order,
                               int pageIndex,
                               int pageSize, boolean withTotal) {
        long startTime = System.currentTimeMillis();
        if (pageIndex <= 0) {
            pageIndex = 1;
        }
        if (pageSize > 200) {
            pageSize = 200;
        }
        if (order == null) {
            order = new Document(_ID, -1);
        }
        if (withTotal) {
            int total = (int) collection.count(where);

            int pageCount = total / pageSize + (total % pageSize > 0 ? 1 : 0);
            if (pageIndex > pageCount) {
                pageIndex = pageCount;
            }
            if (pageIndex <= 0) {
                pageIndex = 1;
            }
            List<Document> list = __list(where, order, pageSize * (pageIndex - 1), pageSize, select);
            long endTime = System.currentTimeMillis();
            return new Document()
                    .append("totalCount", total)
                    .append("pageCount", pageCount)
                    .append("pageIndex", pageIndex)
                    .append("pageSize", pageSize)
                    .append(this.listName, list)
                    .append("currentTime", endTime)
                    .append("timeUsed", endTime - startTime);
        } else {
            List<Document> list = __list(where, order, pageSize * (pageIndex - 1), pageSize + 1, select);
            boolean hasMore = list.size() > pageSize;
            if (hasMore) {
                // 删除最后一项
                list.remove(pageSize);
            }
            long endTime = System.currentTimeMillis();
            return new Document(Key.RETURN_CODE_TAG, "OK")
                    .append("pageIndex", pageIndex)
                    .append("pageSize", pageSize)
                    .append("hasMore", hasMore)
                    .append(this.listName, list)
                    .append("currentTime", endTime)
                    .append("timeUsed", endTime - startTime);
        }
    }

    // 取整下限
    public static int floorDiv(int x, int y) {
        int r = x / y;
        // 同号, round down
        if (r * y == x) {
            r--;
        }
        return r;
    }

    public static int ceilDiv(int x, int y) {
        int r = x / y;
        // if the signs are different and modulo not zero, round down
        if (r * y != x) {
            r++;
        }
        return r;
    }

    public Document __page2(
            Document $match,
            Document $project,
            final Document $sort,
            final int pageIndex,
            final int pageSize) {
        final Document out = new Document(Key.RETURN_CODE_TAG, Key.OK);
        long startTime = System.currentTimeMillis();
        List<Document> pipeline = new ArrayList<>();
        if ($match != null && !$match.isEmpty()) {
            pipeline.add(new Document("$match", $match));
        }
        if ($project != null && !$project.isEmpty()) {
            pipeline.add(new Document("$project", $project));
        }
        pipeline.add(new Document("$group", new Document("_id", 1).append("count", new Document("$sum", 1))));
        final MongoCollection<Document> table = this.collection;
        table.aggregate(pipeline).forEach(new Block<Document>() {
            @Override
            public void apply(final Document document) {
                int totalCount = document.get("count", Integer.class);
                if (totalCount <= 0) {
                    return;
                }
                System.err.println("p2->" + (System.currentTimeMillis() - startTime));
                int pSize = pageSize < 0 || pageSize > 200 ? 200 : pageSize;
                int pCount = ceilDiv(totalCount, pSize);
                int pIndex = pageIndex;
                if (pIndex <= 0) {
                    pIndex = 1;
                } else if (pIndex > pCount) {
                    pIndex = pCount; // 超出范围自动定位到最后一页
                }
                pipeline.remove(pipeline.size() - 1); // 去除掉 "$group" 部分
                if ($sort != null && !$sort.isEmpty()) {
                    pipeline.add(new Document("$sort", $sort));
                }
                int $skip = (pIndex - 1) * pSize;
                if ($skip > 0) {
                    pipeline.add(new Document("$skip", (pIndex - 1) * pageSize));
                }
                pipeline.add(new Document("$limit", pSize));
                out.append("totalCount", totalCount)
                        .append("pageCount", pCount)
                        .append("pageIndex", pIndex)
                        .append("pageSize", pSize)
                        .append(listName, table.aggregate(pipeline));
            }
        });
        long endTime = System.currentTimeMillis();
        out.append("currentTime", endTime);
        out.append("timeUsed", endTime - startTime);
        return out;
    }

    // 目前
    /**
    public Document lookup(Document input, Document user) {
        String key = Docat.getString(input, "key");
        Runtimes.throwIf(Strings.isEmptyStrictly(Key.ID), "输入格式错误，未找到key");
        Lookup lookup = this.lookupMap.get(key);

        Runtimes.throwIf(lookup == null, "输入格式错误，未找到lookup配置");
        //查询基本数据
        Object _id = __ID(input.get(_ID));
        Document where = new Document(_ID, _id);
        if (this.isPublic()) {
            // public 读取不做任何限制
        } else if (user != null) {
            if (user.getInteger("role", 0) >= 8) {
                // 可以访问
            } else {
                // 如果权限未通过的话就判断记录是否属于该用户
                where.append(Key._USER, user.get(_ID));
            }
        }
        Document item = this.__get(where, null, Documents.__select(key));
        Runtimes.throwIf(item == null, "未找到记录");
        Object _lid = item.get(key);
        if (_lid == null) {
            return new Document(Key.RETURN_CODE_TAG, "OK").append(lookup.KeyObject.itemName, lookup.unsetValue);
        }

        Document select = Docat.getDocument(input, "select");

        if (!lookup.isItem) { // 处理单值类型的isList冲突，将单值转换为列表
            if (!(_lid instanceof List || _lid instanceof Object[])) {
                List l = new ArrayList();
                l.add(_lid);
                _lid = l;
            }
        }

        if (_lid instanceof List) {
            List l = this.__lookup_by_ids(_lid, select);
            // 丢失值未做处理，是否自动补充(autoAppend)称为missedValue?
            return new Document(Key.RETURN_CODE_TAG, "OK").append(lookup.KeyObject.listName, l);
        } else {
            Document i = lookup.KeyObject.__get_by_id(_lid, select);
            if (i == null) {
                return new Document(Key.RETURN_CODE_TAG, "OK").append(lookup.KeyObject.itemName, lookup.missedValue);
            }
            return new Document(Key.RETURN_CODE_TAG, "OK").append(lookup.KeyObject.itemName, i);
        }
    }**/

    private void countUp(Document v0, Document v1, String field, List<String> closeFieldList) {
        String key = field + "Count";
        v0.append(key, Docat.getInteger(v0, key, 0) + Docat.getInteger(v1, key, 0));
        for (String s : closeFieldList) {
            String k = s + "Count";
            v0.append(k, Docat.getInteger(v0, k, 0) + Docat.getInteger(v1, k, 0));
        }
    }

    // 专用于数据统计
    public Document distinctCount(Document input, Document user) {
        Document where = (Document) input.get("where");
        Document order = (Document) input.get("order");
        String field = Docat.getString(input, "field");
        Document mappingDoc = Docat.getDocument(input, "mapping");
        List<String> closeFieldList = Docat.getList(input, "closeFields");
        Runtimes.throwIf(field == null || field.trim().isEmpty(), "参数错误，缺少 field");

        List<Document> list = this.__distinct_count_2(field, where, order != null ? order : Documents.__order_up(_ID), closeFieldList);
        Runtimes.throwIf(list == null, "访问distinct_count失败");

//        for (Document d : list) {
//            Object v = d.get(_ID);
//            Document df = this.getFieldSpecsMapedDoc(field, v);
//            if (df != null) {
//                d.putAll(df);
//            }
//        }

        // 处理常用值
//        mappingDoc = mappingDoc != null ? mappingDoc : this.getFieldSpecsCommonValuesDoc(field);
        if (mappingDoc == null || mappingDoc.isEmpty()) {
            return new Document(Key.RETURN_CODE_TAG, Key.OK).append(this.listName, list);
        } else {
            List<Document> newList = new ArrayList();
            List keyList = Docat.getList(mappingDoc, "keys");
            List valueList = Docat.getList(mappingDoc, "values");
            Runtimes.throwIf(keyList == null || keyList.isEmpty(), "FieldSpecs错误，未定义main");
            Object other = Docat.get(mappingDoc, "other");


            // 映射
            Map<Object, Document> map = new HashMap();
            Set dirtySet = new HashSet();
            for (int i = 0; i < keyList.size(); i++) {
                Object k = keyList.get(i);
                Object v = k;
                if (valueList != null && valueList.size() > i) {
                    v = valueList.get(i);
                }
                Document d = new Document(_ID, k).append(field, v);
                newList.add(d);
                map.put(k, d);
            }

            Document otherDoc = other == null ? null : new Document(_ID, other).append(field, other);
            if (otherDoc != null) {
                newList.add(otherDoc);
            }
            for (Document doc : list) {
                Object k = Docat.get(doc, _ID);
                Document d = map.get(k);
                if (d != null) {
                    this.countUp(d, doc, field, closeFieldList);
                    dirtySet.add(k);
                } else if (otherDoc != null) {
                    this.countUp(otherDoc, doc, field, closeFieldList);
                    dirtySet.add(other);
                }
            }
            for (int i = newList.size() - 1; i >= 0; i--) {
                Document doc = newList.get(i);
                Object k = Docat.get(doc, _ID);
                if (!dirtySet.contains(k)) {
                    newList.remove(i);
                }
            }
            return new Document(Key.RETURN_CODE_TAG, Key.OK).append(this.listName, newList);
        }
    }
    public Document quickSearch(Document input, Document user) {
        List<String> fieldList = Docat.getList(input, "fields");
        Runtimes.throwIf(fieldList == null || fieldList.isEmpty(), "参数错误，缺少 fields");
        Document where = (Document) input.get("where");
        Document item = new Document();
        for (String field : fieldList) {
            List<Document> list = this.__distinct_count(field, where, Documents.__order_down("count"));
            List newList = new ArrayList();
            for (Document doc : list) {
                Object _id = Docat.get(doc, _ID);
                if (_id != null) {
                    int count = Docat.getInteger(doc, "count");
                    List one = new ArrayList();
                    one.add(_id);
                    one.add(count);
                    newList.add(one);
                }
            }
            item.append(field, newList);
        }
        return new Document(Key.RETURN_CODE_TAG, Key.OK).append(this.itemName, item);
    }

    public Document aggregate(Document input, Document user) {
        List<Document> pipeline = Docat.getList(input, "pipeline");

        Document output = new Document(Key.RETURN_CODE_TAG, Key.OK).append(this.listName, this.__aggregate(pipeline));
        return output;
    }

}
