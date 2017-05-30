package cn.edu.hrbeu.mongo.shell;

import cn.edu.hrbeu.mongo.shell.util.Docat;
import cn.edu.hrbeu.mongo.shell.util.Documents;
import org.bson.Document;

import java.util.*;

/**
 * Created by wu on 2017/5/26.
 */
public class ForeignCollection extends Collection {

    public static class Foreign {
        final ForeignCollection collection; // cache
        final String localField; // 目标字段
        final String[] selectFields; // 扩展字段
        final boolean forItem;
        final boolean forList;

        public Foreign(ForeignCollection collection, String localField, String[] selectFields, boolean forItem, boolean forList) {
            this.collection = collection;
            this.localField = localField;
            this.selectFields = selectFields;
            this.forItem = forItem;
            this.forList = forList;
        }
    }

    private static class Lookup {
        final String key; // 目标Cache名称
        final ForeignCollection foreignCollection; // collection
        final boolean isItem; // 目标字段
        final Object unsetValue;
        final Object missedValue;

        public Lookup(String key, ForeignCollection foreignCollection, boolean isItem, Object unsetValue, Object missedValue) {
            this.key = key;
            this.foreignCollection = foreignCollection;
            this.isItem = isItem;
            this.unsetValue = unsetValue;
            this.missedValue = missedValue;
        }
    }

    private boolean isPublic = false;
    private boolean hasParent = false; // 内联表 _pid = thisTable._id
    private boolean hasFarther = false; // 外联表 _fid = someTable.id
    private String fatherName;
    private boolean fatherAuto = false;
    private OperationWithUser fatherCollection = null;
    private final List<Foreign> foreignList = new ArrayList();
    private final List<Foreign> listForeignList = new ArrayList();
    private final List<Foreign> itemForeignList = new ArrayList();
    private final Map<String, Lookup> lookupMap = new HashMap<String, Lookup>();
    protected final List<String> defaultSearchFields = new ArrayList();
    protected final Document fieldSpecs = new Document();

    public ForeignCollection(String collectionName, int keyType, Connection conn) {
        super(collectionName, keyType, conn);
        this.defaultSearchFields.add("name");
    }

    public boolean isPublic() {
        return isPublic;
    }

    public void setPublic(boolean aPublic) {
        isPublic = aPublic;
    }

    public boolean isHasParent() {
        return hasParent;
    }

    public void setHasParent(boolean hasParent) {
        this.hasParent = hasParent;
    }

    public boolean isHasFarther() {
        return hasFarther;
    }

    public void setHasFarther(boolean hasFarther) {
        this.hasFarther = hasFarther;
    }

    public void setFather(String name, OperationWithUser fatherCollection, boolean auto) {
        this.fatherName = name.trim();
        this.fatherCollection = fatherCollection;
        this.fatherAuto = auto;
    }
    public void addForeign(ForeignCollection collection, String localField, String[] selectFields, boolean forItem, boolean forList) {
        Foreign foreign = new Foreign(collection, localField, selectFields, forItem, forList);
        this.addForeign(foreign);
        if (forItem) {
            this.addItemForeign(foreign);
        }
        if (forList) {
            this.addListForeign(foreign);
        }
    }

    private void addForeign(Foreign foreign) {
        for (int i = 0; i < this.foreignList.size(); i++) {
            Foreign f = this.foreignList.get(i);
            if (f.localField.equals(foreign.localField)) {
                this.foreignList.set(i, f);
                return;
            }
        }
        this.foreignList.add(foreign);
    }

    private void addItemForeign(Foreign foreign) {
        for (int i = 0; i < this.itemForeignList.size(); i++) {
            Foreign f = this.itemForeignList.get(i);
            if (f.localField.equals(foreign.localField)) {
                this.itemForeignList.set(i, foreign);
                return;
            }
        }
        this.itemForeignList.add(foreign);
    }

    private void addListForeign(Foreign foreign) {
        for (int i = 0; i < this.listForeignList.size(); i++) {
            Foreign f = this.listForeignList.get(i);
            if (f.localField.equals(foreign.localField)) {
                this.listForeignList.set(i, foreign);
                return;
            }
        }
        this.listForeignList.add(foreign);
    }

    public void addLookup(String key, ForeignCollection foreignCollection, boolean isItem, Object unsetValue, Object missedValue) {
        Lookup lookup = new Lookup(key, foreignCollection, isItem, unsetValue, missedValue);
        this.lookupMap.put(key, lookup);
    }

    public void setDefaultSearchFields(String... keys) {
        if (keys != null && keys.length > 0) {
            this.defaultSearchFields.clear();
            for (String key : keys) {
                this.defaultSearchFields.add(key);
            }
        }
    }

    public void setDefaultSearchFields(List<String> keyList) {
        if (keyList != null && keyList.size() > 0) {
            this.defaultSearchFields.clear();
            this.defaultSearchFields.addAll(keyList);
        }
    }

    public boolean appendForeignKey(List list, String[] pathSeq, String... fields) {
        if (list == null) {
            return true;
        }
        // 保存path指定的最后一个JSON对象
        List<Document> lastObjectList = new ArrayList();
        // 要查找的_id集合
        HashSet keySet = new HashSet();
        // 获取要查找的_id集合
        for (int i = 0; i < list.size(); i++) {
            Document item = (Document) list.get(i);
            // 最后一个 JSON 元素
            Document lastObject = Documents.getLastDocumentByPathSeq(item, false, pathSeq);
            if (lastObject != null) {
                Object foreignId = lastObject.get(pathSeq[pathSeq.length - 1]);
                if (foreignId != null) {
                    lastObjectList.add(lastObject);
                    if (foreignId instanceof List) {
                        // keySet.addAll((BasicDBList) value);
                        for (Object v : (List) foreignId) {
                            if (v instanceof Document || v instanceof List) {
                                throw new RuntimeException("仅支持数值或字符串类型的ForeignKey");
                            }
                            keySet.add(v);
                        }
                    } else if (foreignId instanceof Document) {
                        throw new RuntimeException("仅支持数值或字符串类型的ForeignKey");
                    } else {
                        keySet.add(foreignId);
                    }
                }
            }
        }
        List idList = new ArrayList();
        idList.addAll(keySet);
        // 读取所有的数据到map中
        Map map = this.__mapByIds(idList, fields);

        // 将读取的结果影射到对应数值
        for (Document lastObject : lastObjectList) {
            Object foreignId = lastObject.get(pathSeq[pathSeq.length - 1]);
            if (foreignId instanceof List) { // 处理数组型外键
                List newList = new ArrayList();
                for (Object k : (List) foreignId) {
                    Object v = map.get(k);
                    if (v == null) {
                        v = new Document(_ID, k).append("name", "missed!"); // _ID丢失
                    }
                    newList.add(v);
                }
                lastObject.put(pathSeq[pathSeq.length - 1], newList); // 替换原有值
            } else { // 处理数值型外键
                Object v = map.get(foreignId);
                if (v == null) {
                    v = new Document(_ID, foreignId).append("name", "missed!"); // _ID丢失
                }
                lastObject.put(pathSeq[pathSeq.length - 1], v); // 替换原有值
            }
        }

        return true;
    }

    public boolean appendForeignKey(List list, String dotPath, String... fields) {
        if (list == null) {
            return true;
        }
        String[] path = dotPath.split("\\.");
        return this.appendForeignKey(list, path, fields);
    }

    public boolean appendForeignKey(Document item, String[] pathSeq, String... fields) {
        if (item == null) {
            return true;
        }
        Document lastObject = Documents.getLastDocumentByPathSeq(item, false, pathSeq);
        if (lastObject != null) {
            Object foreignId = lastObject.get(pathSeq[pathSeq.length - 1]);
            if (foreignId != null) {
                if (foreignId instanceof List) {
                    List list = (List) foreignId;
                    if (!list.isEmpty()) {
                        for (Object v : list) { // 检查外键值类型类型
                            if (v instanceof Document || v instanceof List) {
                                throw new RuntimeException("不支持Document或List类型的ForeignKey");
                            }
                        }
                        // 获取外键数据
                        Map map = this.__mapByIds(list, fields);
                        List newList = new ArrayList();
                        for (Object k : list) {
                            Object v = map.get(k);
                            if (v == null) {
                                v = new Document(_ID, k).append("name", "missed!"); // _ID丢失
                            }
                            newList.add(v);
                        }
                        lastObject.put(pathSeq[pathSeq.length - 1], newList); // 替换原有值
                    }
                } else if (foreignId instanceof Document) {
                    throw new RuntimeException("不支持Document或List类型的ForeignKey");
                } else {
                    // 直接通过ID获取数据
                    Object v = this.__get_by_id(foreignId, fields);
                    if (v == null) {
                        v = new Document(_ID, foreignId).append("name", "missed!"); // _ID丢失
                    }
                    lastObject.put(pathSeq[pathSeq.length - 1], v); // 替换原有值
                }
            }
        }
        return true;
    }

    public boolean appendForeignKey(Document item, String pathWithDots, String... fields) {
        if (item == null) {
            return true;
        }
        String[] path = pathWithDots.split("\\.");
        return this.appendForeignKey(item, path, fields);
    }

    public void appendAllForeignKey(Document item) {
        for (Foreign f : this.itemForeignList) {
            if (f != null && f.collection != null) {
                f.collection.appendForeignKey(item, f.localField, f.selectFields);
            }
        }
    }

    public void appendAllForeignKey(List list) {
        for (Foreign f : this.listForeignList) {
            if (f != null && f.collection != null) {
                f.collection.appendForeignKey(list, f.localField, f.selectFields);
            }
        }
    }

    // 对 append 指定的字段进行扩展
    public void appendForeign(Document item, Document append) {
        for (Foreign f : this.itemForeignList) {
            boolean b = true;
            if (append != null && !Docat.getBoolean(append, f.localField)) {
                b = false;
            }
            if (b && f.collection != null) {
                f.collection.appendForeignKey(item, f.localField, f.selectFields);
            }
        }
    }

    // 对 append 指定的字段进行扩展
    public void appendForeign(List list, Document append) {
        for (Foreign f : this.listForeignList) {
            boolean b = true;
            if (append != null && !Docat.getBoolean(append, f.localField)) {
                b = false;
            }
            if (b && f.collection != null) {
                f.collection.appendForeignKey(list, f.localField, f.selectFields);
            }
        }
    }

    // 用于数据导出
    public Map mapForeignKey(Document item, String pathWidthDots, String... fields) {
        String[] pathSeq = pathWidthDots.split("\\.");
        return this.mapForeignKey(item, pathSeq, fields);
    }

    // 获取所有外键值，放在map中，用于数据导出
    public void __foreign_key_map(Document item, boolean includeSelf, Map<String, Set> map) {
        if (includeSelf) {
            Set set = map.get(this.collectionName);
            if (set == null) {
                set = new HashSet();
                map.put(this.collectionName, set);
            }
            set.add(item.get(_ID));
        }
        for (Foreign f : this.foreignList) {
            String[] pathSeq = f.localField.split("\\.");
            Object lastObject = Documents.getValueByPathSeq(item, pathSeq);
            if (lastObject != null && !(lastObject instanceof Document)) {
                Set set = map.get(f.collection.collectionName);
                if (set == null) {
                    set = new HashSet();
                    map.put(f.collection.collectionName, set);
                }
                if (lastObject instanceof List) {
                    set.addAll((List) lastObject);
                } else {
                    set.add(lastObject);
                }
            }
        }
    }

    // 找到 foreign key 对应的值映射
    public Map mapForeignKey(Document item, String[] pathSeq, String... fields) {
        if (item == null) {
            return null;
        }
        Document lastObject = Documents.getLastDocumentByPathSeq(item, false, pathSeq);
        if (lastObject != null) {
            Object foreignId = lastObject.get(pathSeq[pathSeq.length - 1]);
            if (foreignId != null) {
                if (foreignId instanceof List) {
                    List list = (List) foreignId;
                    if (!list.isEmpty()) {
                        for (Object v : list) { // 检查外键值类型类型
                            if (v instanceof Document || v instanceof List) {
                                throw new NullPointerException("仅支持数值或字符串类型的ForeignKey");
                            }
                        }
                        // 获取外键数据
                        Map map = this.__mapByIds(list, fields);
                        return map;
                    }
                } else if (foreignId instanceof Document) {
                    throw new NullPointerException("仅支持数值或字符串类型的ForeignKey");
                } else {
                    // 直接通过ID获取数据
                    Document v = this.__get_by_id(foreignId, fields);
                    if (v != null) {
                        HashMap map = new HashMap();
                        map.put(v.get(_ID), v);
                        return map;
                    }
                }
            }
        }
        return null;
    }

    public void setFieldSpecs(Document specs) {
        if (specs == null || specs.isEmpty()) {
            return;
        }
        this.fieldSpecs.putAll(specs);
        String[] keys = this.fieldSpecs.keySet().toArray(new String[0]);
        for (String key : keys) {
            Document doc = Docat.getDocument(this.fieldSpecs, key);
            if (doc != null) {
                if (!doc.containsKey("_mappingTable")) {
                    List<Document> mapList = Docat.getList(doc, "mappingTable");
                    if (mapList != null && !mapList.isEmpty()) {
                        Map _map = new HashMap();
                        for (Document d : mapList) {
                            Object k = d.get("key");
                            if (k != null) {
                                d.remove("key");
                                _map.put(k, d);
                            }
                        }
                        doc.append("_mappingTable", _map);
                    }
                }
            } else {
                this.fieldSpecs.remove(key);
            }
        }
    }


    // 获取字段值所对应的定义
    public Document getFieldSpecsMapedDoc(String field, Object value) {
        Document fieldDoc = Docat.getDocument(fieldSpecs, field);
        if (fieldDoc != null) {
            Map _map = (Map) fieldDoc.get("_mappingTable");
            if (_map != null) {
                return (Document) _map.get(value);
            }
        }
        return null;
    }

    // 获取值映射的定义
    public Document getFieldSpecsMappingTableDoc(String field, Object value) {
        Document fieldDoc = Docat.getDocument(fieldSpecs, field);
        if (fieldDoc != null) {
            return Docat.getDocument(fieldDoc, "mappingTable");
        }
        return null;
    }

    // 获取字段值所对应的定义
    public Document getFieldSpecsCommonValuesDoc(String field) {
        Document fieldDoc = Docat.getDocument(fieldSpecs, field);
        if (fieldDoc != null) {
            return Docat.getDocument(fieldDoc, "commonValues");
        }
        return null;
    }

    public interface DocumentChangeListener {
        public void onDoumentChanged(String name, Document doc);
    }

    private final List<DocumentChangeListener> listeners = Collections.synchronizedList(new LinkedList<DocumentChangeListener>());

    public void addDocumentChangeListener(DocumentChangeListener l) {
        if (!listeners.contains(l)) {
            listeners.add(l);
        }
    }

    public void removeDocumentChangeListener(DocumentChangeListener l) {
        listeners.remove(l);
    }

    public void clearDocumentChangeListener() {
        listeners.clear();
    }

    public void fire(String name, Document value) {
        DocumentChangeListener[] pcls = listeners.toArray(new DocumentChangeListener[0]);
        for (DocumentChangeListener pcl : pcls) {
            pcl.onDoumentChanged(name, value);
        }
    }
}
