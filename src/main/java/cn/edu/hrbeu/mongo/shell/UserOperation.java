package cn.edu.hrbeu.mongo.shell;

import java.util.ArrayList;
import java.util.List;
import cn.edu.hrbeu.mongo.shell.util.Docat;
import cn.edu.hrbeu.mongo.shell.util.D;
import cn.edu.hrbeu.mongo.shell.util.Strings;
import org.bson.Document;
/**
 * Created by wu on 2017/5/26.
 */
public class UserOperation  extends OperationWithUser{

    public UserOperation(int keyType, Connection conn) {
        super("user", keyType, conn, "value", "values");
    }

    @Override
    public Document api(String op, Document input, Document user) {
        Document output = null;
        switch (op) {
            case "resister": {
                if (user != null) {
                    output = new Document(Key.RETURN_CODE_TAG, Key.HAS_LOGINED);
                } else {
                    output = this.register(input);
                }
                break;
            }
            case "login": {
                if (user != null) {
                    output = new Document(Key.RETURN_CODE_TAG, Key.HAS_LOGINED);
                } else {
                    output = this.login(input);
                }
                break;
            }
            case "list": {
                if (user == null) {
                    output = new Document(Key.RETURN_CODE_TAG, Key.NOT_LOGINED);
                } else {
                    output = this.getList(input, user);
                }
                break;
            }
            case "get": {
                if (user == null) {
                    output = new Document(Key.RETURN_CODE_TAG, Key.NOT_LOGINED);
                } else {
                    if (!input.containsKey(Key._ID)) {
                        input.append(Key._ID, user.get(Key._ID));
                    }
                    output = this.getItem(input, user);
                }
                break;
            }
            case "password": {
                if (user == null) {
                    output = new Document(Key.RETURN_CODE_TAG, Key.NOT_LOGINED);
                } else {
                    // 权限判断
                    output = this.password(input, user);
                }
                break;
            }
            case "update": {
                if (user == null) {
                    output = new Document(Key.RETURN_CODE_TAG, Key.NOT_LOGINED);
                } else {
                    output = this.updateItem(input, user);
                }
                break;
            }
            case "insert": {
                if (user == null) {
                    output = new Document(Key.RETURN_CODE_TAG, Key.NOT_LOGINED);
                } else if (user.getInteger("role", 0) != 9) {
                    output = new Document(Key.RETURN_CODE_TAG, Key.NO_AUTHORITY);
                } else {
                    output = this.insertItem(input, user);
                }
                break;
            }
            case "delete": {
                if (user == null) {
                    output = new Document(Key.RETURN_CODE_TAG, Key.NOT_LOGINED);
                } else {
                    output = this.deleteItem(input, user);
                }
                break;
            }
            case "deletemany": {
                output = this.deleteMany(input, user);
                break;
            }
            case "push": {
                if (user == null) {
                    output = new Document(Key.RETURN_CODE_TAG, Key.NOT_LOGINED);
                } else if (user.getInteger("role", 0) != 9) {
                    output = new Document(Key.RETURN_CODE_TAG, Key.NO_AUTHORITY);
                } else {
                    output = this.pushItem(input, null);
                }
                break;
            }
            case "pull": {
                if (user == null) {
                    output = new Document(Key.RETURN_CODE_TAG, Key.NOT_LOGINED);
                } else if (user.getInteger("role", 0) != 9) {
                    output = new Document(Key.RETURN_CODE_TAG, Key.NO_AUTHORITY);
                } else {
                    output = this.pullItem(input, user);
                }
                break;
            }
        }
        return output;
    }



    public Document login(Document input) {
        Document user = (Document) input.get(this.itemName);

        String name = user.getString("name");
        String password = user.getString("password");
        if (!Strings.isCompactedWorld(name) || !Strings.isCompactedWorld(password)) {
            return new Document(Key.RETURN_CODE_TAG, "输入格式错误！");
        }
        Document where = new Document("password", password).append("deleted", new Document("$ne", true));
        if (Strings.isEmail(name)) {
            where.append("email", name);
        } else {
            where.append("name", name);
        }
        Document item = this.__get(where, null, new Document("password", 0));
        if (item != null) {
            // 找上次登录时间
            // ...
            // 记录登录状态
            if (item.getInteger("role", 0) == 0) {
                return new Document(Key.RETURN_CODE_TAG, "未授权用户不可登陆，请等待授权后再次尝试。");
            }
            return new Document(Key.RETURN_CODE_TAG, "OK").append(this.itemName, item);
        } else {
            return new Document(Key.RETURN_CODE_TAG, "账户/密码错误！");
        }
    }

    public Document register(Document input) {
        Document item = (Document) input.get(this.itemName);
        String name = item.getString("name");
        String email = item.getString("email");
        String password = item.getString("password");
        if (!Strings.isCompactedWorld(name) || !Strings.isCompactedWorld(email) || !Strings.isCompactedWorld(password)) {
            return new Document(Key.RETURN_CODE_TAG, "格式错误");
        }
        if (!Strings.isEmail(email)) {
            return new Document(Key.RETURN_CODE_TAG, "EMail格式错误");
        }
        // 检查冲突
        List list = new ArrayList();
        list.add(new Document("name", name));
        list.add(new Document("email", email));
        Document where = new Document("$or", list);
        Document output = this.__get(where, null, new Document("_id", 1));
        if (output != null) {
            return new Document(Key.RETURN_CODE_TAG, "用户名或Email冲突");
        }
        Document values = new Document("name", name)
                .append("email", email)
                .append("password", password)
                .append("role", 0);
        item = __insert_one(null, values, System.currentTimeMillis());
        if (item != null) {
            long count = this.__count(null);
            if (count == 1) {
                // 第一个入住的用户是Admin权限
                D.trace("第一个入住的用户是Admin权限");
                this.__set_by_id(item.get(_ID), new Document("role", 9), System.currentTimeMillis());
            }
            return new Document(Key.RETURN_CODE_TAG, "OK").append(this.itemName, item);
        } else {
            return new Document(Key.RETURN_CODE_TAG, "注册失败");
        }
    }

    /**
     * 插入
     *
     * @param input {zhenyuan:{...}}
     * @param user
     * @return
     */
    @Override
    public Document insertItem(Document input, Document user) {
        Document item = (Document) input.get(this.itemName);

        if (item == null) {
            return new Document(Key.RETURN_CODE_TAG, "输入格式错误");
        }
        if (item.containsKey(_ID)) {
            return new Document(Key.RETURN_CODE_TAG, "输入格式错误");
        }
        if (user == null) {
            return new Document(Key.RETURN_CODE_TAG, "用户错误");
        }
        Object _uid = user.get(_ID);
        item.append(Key._USER, _uid);
        item.append("role", 0);
        Document output = this.__insert_one(null, item, System.currentTimeMillis());
        if (output != null) {
            this.appendAllForeignKey(output);
            return new Document(Key.RETURN_CODE_TAG, "OK").append(this.itemName, output);
        } else {
            return new Document(Key.RETURN_CODE_TAG, "未找到记录");
        }

    }

    /**
     * 删除
     *
     * @param input {value:{_id:1}}
     * @param user
     * @return
     */
    @Override
    public Document deleteItem(Document input, Document user) {
        if (user == null) {
            return new Document(Key.RETURN_CODE_TAG, "未登录用户");
        }
        Object _uid = user.get(_ID);
        if (user.getInteger("role", 0) != 9) {
            return new Document(Key.RETURN_CODE_TAG, "用户没有管理权限");
        }
        //查询基本数据
        Object _id = this.__ID(input.get(_ID));
        if (_uid.equals(_id)) {
            return new Document(Key.RETURN_CODE_TAG, "用户不能删除自身");
        }

        Document item = this.__get_by_id(_id);
        if (item == null) {
            return new Document(Key.RETURN_CODE_TAG, "要删除的用户不存在");
        }
        if (item.getInteger("role", 0) == 9) {
            return new Document(Key.RETURN_CODE_TAG, "不能删除高级用户");
        }

        item = this.__update_by_id(_id, new Document("deleted", true).append("deleteTime", System.currentTimeMillis()), 0);
        if (item != null) {
            return new Document(Key.RETURN_CODE_TAG, "OK");
        } else {
            return new Document(Key.RETURN_CODE_TAG, "删除失败");
        }
    }

    @Override
    public Document deleteMany(Document input, Document user) {
        if (user == null) {
            return new Document(Key.RETURN_CODE_TAG, "未登录用户");
        }
        Object _uid = user.get(_ID);
        if (user.getInteger("role", 0) != 9) {
            return new Document(Key.RETURN_CODE_TAG, "用户没有管理权限");
        }

        //查询基本数据
        List ids = input.get("_ids", List.class);
        if (ids == null || ids.isEmpty()) {
            return new Document(Key.RETURN_CODE_TAG, "输入格式错误，ids is null or empty");
        }
        for (int i = 0; i < ids.size(); i++) {
            Object _id = this.__ID(ids.get(i));
            if (_uid.equals(_id)) {
                return new Document(Key.RETURN_CODE_TAG, "用户不能删除自身");
            }
            ids.set(i, _id);
        }

        Document where = new Document(_ID, new Document("$in", ids));
        int count = (int) this.__update_many(where, new Document("deleted", true).append("deleteTime", System.currentTimeMillis()), 0);
        return new Document(Key.RETURN_CODE_TAG, "OK").append("count", count);
    }

    @Override
    public Document updateItem(Document input, Document user) {
        Object _id = this.__ID(input.get(_ID));
        Document values = (Document) input.get(this.itemName);
        if (values == null || values.containsKey(_ID)) {
            return new Document(Key.RETURN_CODE_TAG, "输入格式错误 value has id");
        }
        Document select = (Document) input.get("select");
        if (select == null) {
            select = new Document("password", 0);
        } else {
            select.append("password", 0);
        }

        Object _uid = null;
        if (user != null) {
            _uid = user.get(_ID);
            if (user.getInteger("role", 0) == 9) {
                // 可以访问
//                M.trace("Admin可以修改");
            } else if (_id.equals(user.get(_ID))) {
                // 自己可以修改自己可以访问
//                M.trace("自己可以修改自己");
            } else {
                // 不可以修改别人
                return new Document(Key.RETURN_CODE_TAG, "无权限");
            }
        }

        Document item = this.__update_by_id(_id, values, System.currentTimeMillis(), select);
        if (item != null) {
            this.appendAllForeignKey(item);
            return new Document(Key.RETURN_CODE_TAG, "OK").append(this.itemName, item);
        } else {
            return new Document(Key.RETURN_CODE_TAG, "修改失败");
        }
    }

    @Override
    public Document getItem(Document input, Document user) {
        //查询基本数据
        Object _id = this.__ID(input.get(_ID));
        Document where = new Document(_ID, _id);
        Document select = new Document("password", 0);
        if (user != null) {
            if (user.getInteger("role", 0) == 9) {
                // 可以访问
            } else if (_id.equals(user.get(_ID))) {
                // 可以访问
            } else {
                // 只可以访问_id和name
                select.append(_ID, 1).append("name", 1);
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
     * 列表
     *
     * @param input where select order pageSize pageIndex 注：模糊查询 例
     * select:{name:{$regex:'aa'}}
     * @param user
     * @return
     */
    @Override
    public Document getList(Document input, Document user) {
        if (user != null) {
            if (user.getInteger("role", 0) == 9) {
                // 可以访问
            } else {
                return new Document(Key.RETURN_CODE_TAG, "无权限");
            }
        }
        Document select = new Document("password", 0);
        Document where = Docat.getDocument(input, "where", new Document());
        where.append("deleted", new Document("$ne", true));
        input.append("where", where);
        input.append("select", select);
        return super.getList(input, user);
    }

    public Document password(Document input, Document user) {
        Document value = (Document) input.get(this.itemName);
        if (value == null) {
            return new Document(Key.RETURN_CODE_TAG, "输入格式错误");
        }
        Object _id = this.__ID(input.get(_ID));

        Object _uid = null;
        if (user != null) {
            _uid = this.__ID(user.get(_ID));
            if (user.getInteger("role", 0) == 9) {
                // 可以访问
            } else if (_id.equals(user.get(_ID))) {
                // 自己可以修改自己可以访问
            } else {
                // 不可以修改别人
                return new Document(Key.RETURN_CODE_TAG, "无权限");
            }
        }

        String password = value.getString("password");
        String newPassword = value.getString("newPassword");
        if (!Strings.isCompactedWorld(password) || !Strings.isCompactedWorld(newPassword)) {
            return new Document(Key.RETURN_CODE_TAG, "输入格式错误");
        }

        Document item = this.__get_by_id(_id, "password");
        if (item == null) {
            return new Document(Key.RETURN_CODE_TAG, "账户不存在");
        }
        if (!password.equals(item.getString("password"))) {
            return new Document(Key.RETURN_CODE_TAG, "原密码输入错误");
        }

        item = this.__update_by_id(_id, new Document("password", newPassword), 0, new Document("password", 0));
        if (item == null) {
            return new Document(Key.RETURN_CODE_TAG, "写入错误");
        } else {
            return new Document(Key.RETURN_CODE_TAG, Key.OK).append(this.itemName, item);
        }
    }
}
