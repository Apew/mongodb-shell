package cn.edu.hrbeu.mongo.shell.util;

import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by wu on 2017/5/25.
 */
public class Documents {
    /**
     * 获取其中任意个一非空数据
     *
     * @param fields
     * @return
     */
    public static Document getOneOfField(Document input, String... fields) {
        for (String field : fields) {
            Object v = input.get(field);
            if (v != null && v instanceof Document) {
                return (Document) v;
            }
        }
        return null;
    }

    //将String[]类型的字段转化为{field1:1,field2:1,...}的Document
    public static Document __select(String... fields) {
        Document select = null;
        if (fields != null && fields.length > 0) {
            select = new Document();
            for (String field : fields) {
                select.append(field, 1);
            }
        }
        return select;
    }

    //将String[]类型的字段转化为{field1:0,field2:0,...}的Document
    public static Document __unselect(String... fields) {
        Document select = null;
        if (fields != null && fields.length > 0) {
            select = new Document();
            for (String field : fields) {
                select.append(field, 0);
            }
        }
        return select;
    }

    // 以fields为增序排列
    public static Document __order_up(String... fields) {
        Document select = null;
        if (fields != null && fields.length > 0) {
            select = new Document();
            for (String field : fields) {
                select.append(field, 1);
            }
        }
        return select;
    }

    // 以fields为降序排列
    public static Document __order_down(String... fields) {
        Document select = null;
        if (fields != null && fields.length > 0) {
            select = new Document();
            for (String field : fields) {
                select.append(field, -1);
            }
        }
        return select;
    }

    // 获得指定路径下的字段值
    public static Object getFieldValue(Document doc, Object defaultValue, String... fieldPath) {
        Document d = doc;
        for (int i = 0; i < fieldPath.length - 1; i++) {
            Object t = d.get(fieldPath[i]);
            if (t instanceof Document) {
                d = (Document) t;
            } else {
                return defaultValue;
            }
        }
        Object o = d.get(fieldPath[fieldPath.length - 1]);
        return o == null ? defaultValue : o;
    }

    // 获得指定路径下的字段值
    public static void removeField(Document doc, String... fieldPath) {
        Document d = doc;
        for (int i = 0; i < fieldPath.length - 1; i++) {
            Object t = d.get(fieldPath[i]);
            if (t instanceof Document) {
                d = (Document) t;
            } else {
                return;
            }
        }
        d.remove(fieldPath[fieldPath.length - 1]);
    }

    // 判断指定路径下的值是否相等
    public static Object equalsFieldValue(Document doc, Object value, String... fieldPath) {
        Object v = getFieldValue(doc, null, fieldPath);
        if (v == null && value == null) {
            return true;
        } else {
            return value.equals(v);
        }
    }

    public static boolean setFieldValue(Document doc, Object value, String... fieldPath) {
        Document d = doc;
        for (int i = 0; i < fieldPath.length - 1; i++) {
            Object t = d.get(fieldPath[i]);
            if (t == null || !(t instanceof Document)) {
                t = new Document();
                d.put(fieldPath[i], t);
            }
            d = (Document) t;
        }
        d.put(fieldPath[fieldPath.length - 1], value);
        return true;
    }

    /**
     * 过滤，0去掉，选取
     *
     * @param doc
     * @param select
     * @return
     */
    public static Document __reject(Document doc, Document select) {
        if (select == null || doc == null || doc.isEmpty() || select.isEmpty()) {
            return doc;
        }
        Document copy = null;
        List<String> removeKeys = null;
        for (Map.Entry<String, Object> entry : select.entrySet()) {
            String key = entry.getKey();
            int s = (int) entry.getValue();
            if (s == 1) {
                if (copy == null) {
                    copy = new Document();
                }
                String[] path = key.split("\\.");
                Object v = getFieldValue(doc, null, path);
                if (v != null) {
                    setFieldValue(copy, v, path);
                }
            } else if (s == 0) {
                if (removeKeys == null) {
                    removeKeys = new ArrayList();
                }
                removeKeys.add(key);
            }
        }
        copy = copy == null ? doc : copy;
        if (removeKeys != null) {
            for (String key : removeKeys) {
                removeField(copy, key.split("\\."));
            }
        }
        return copy;
    }
    // 扁平化
    public static Document delayering(Document doc, String prefix) {
        Document out = new Document();
        for (Map.Entry<String, Object> entry : doc.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            String dkey = prefix == null ? key : prefix + "." + key;
            if (value instanceof Document) {
                out.putAll((Map) delayering((Document) value, dkey));
            } else {
                out.put(dkey, value);
            }
        }
        return out;
    }

    /**
     * 将 Json 对象扁平化
     *
     * @param doc
     * @return info : {name:"zhangsan", age:20} => {"info.name":"zhangsan",
     * "info.age":20}
     */
    public static Document delayering(Document doc) {
        return delayering(doc, null);
    }
    /**
     * 获取指定路径的最后一个JSON对象，用于支持深度获取
     *
     * @param item
     * @param autoFilled 如果路径不全，自动那个补充
     * @param path 指定字段，如["info", "name"]，表示item.info.name字段
     * @return 指定内容 例如：{info:{name:'zhangsan', age:{max:40,min:30}}} path =
     * ["info", "name"] ==> info={...} path = ["info", "age", "min"] ==>
     * age={...}
     */
    public static Document getLastDocumentByPathSeq(Document item, boolean autoFilled, String... path) {
        if (path == null || path.length <= 0) {
            throw new RuntimeException("path[] can't be null or empty");
        }
        for (int i = 0; i < path.length - 1; i++) {
            String p = path[i];
            Object next = item.get(p);
            if (next instanceof Document) {
                item = (Document) next;
            } else if (next == null) {
                if (autoFilled) {
                    next = new Document();
                    item.put(p, next);
                    item = (Document) next;
                } else {
                    return null;
                }
            } else {
                // 该值不是Document，不能作为中间路径
                throw new RuntimeException("path[] 在 " + p + " 处不是JSON对象，不能作为中间路径");
            }
        }
        return item;
    }

    /**
     * 获取由 path 指定 value，用于支持深度获取
     *
     * @param item
     * @param pathSeq 指定字段，如["info", "name"]，表示item.info.name字段
     * @return 指定内容 例如：{info:{name:'zhangsan', age:{max:40,min:30}}} path =
     * ["info", "name"] ==> "zhangsan" path = ["info", "age", "min"] ==> 30
     */
    public static Object getValueByPathSeq(Document item, String... pathSeq) {
        // 获取路径上最后一个Object
        item = getLastDocumentByPathSeq(item, false, pathSeq);
        return item == null ? null : item.get(pathSeq[pathSeq.length - 1]);
    }

    /**
     * 获取记录的指定字段，用于支持深度获取
     *
     * @param item
     * @param dotPath 以'.'区分指定路径，如"info.name"，表示item.info.name字段
     * @return 指定内容 例如：{info:{name:'zhangsan', age:{max:40,min:30}}} path =
     * "info.name" ==> "zhangsan" path = "info.age.min ==> 30
     */
    public static Object getValueByPathWithDots(Document item, String dotPath) {
        String[] path = dotPath.split("\\.");
        return getValueByPathSeq(item, path);
    }

    /**
     * 设置记录的指定字段，用于支持深度设置
     *
     * @param item
     * @param value 数值
     * @param autoFilled 如果path中任何一个环节不存在就自动补全
     * @param path 指定字段，如["info", "name"]，表示item.info.name字段
     * @return 是否设置成功 例如：{info:{name:'zhangsan', age:{max:40,min:30}}} path =
     * ["info", "name"] ==> "zhangsan" path = ["info", "age", "min"] ==> 30
     */
    public static boolean setValueByPath(Document item, Object value, boolean autoFilled, String... path) {
        // 获取路径上最后一个Object
        item = getLastDocumentByPathSeq(item, autoFilled, path);
        if (item != null) {
            item.put(path[path.length - 1], value);
            return true;
        } else {
            return false;
        }
    }

    /**
     * 设置记录的指定字段，用于支持深度设置
     *
     * @param item
     * @param value 数值
     * @param autoFilled 如果path中任何一个环节不存在就自动补全
     * @param dotPath 以'.'区分指定路径，如"info.name"，表示item.info.name字段
     * @return 是否设置成功 例如：{info:{name:'zhangsan', age:{max:40,min:30}}} path =
     * "info.name" ==> "zhangsan" path = "info.age.min" ==> 30
     */
    public static boolean setValueByDotPath(Document item, Object value, boolean autoFilled, String dotPath) {
        String[] path = dotPath.split("\\.");
        return setValueByPath(item, value, autoFilled, path);
    }

    /**
     * 设置记录的指定字段，用于支持深度设置，并且自动补全路径
     *
     * @param item
     * @param value 数值
     * @param dotPath 以'.'区分指定路径，如"info.name"，表示item.info.name字段
     * @return 是否设置成功 例如：{info:{name:'zhangsan', age:{max:40,min:30}}} path =
     * "info.name", value="lisi" ==> {info:{name:'lisi', age:{max:40,min:30}}}
     * path = "info.age.min", value=50 ==> {info:{name:'zhangsan',
     * age:{max:40,min:50}}}
     */
    public static boolean setValueByDotPath(Document item, Object value, String dotPath) {
        String[] path = dotPath.split("\\.");
        return setValueByPath(item, value, true, path);
    }


}
