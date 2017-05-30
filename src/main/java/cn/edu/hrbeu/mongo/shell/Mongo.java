package cn.edu.hrbeu.mongo.shell;

import cn.edu.hrbeu.mongo.shell.util.*;
import org.bson.Document;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by wu on 2017/5/25.
 */
public class Mongo {

    public static int defaultKeyType = Collection.COLLECTION_TYPE_MONGO_OID;
    public static Document defaultFieldSpecs = new Document();
    private static Connection connection;

    private static final HashMap<String, OperationWithUser> operationMap = new HashMap<String, OperationWithUser>();

    public static boolean conn(String dbAddress,int port, String dbName, String userName, String userPwd, String userDb) {
        if (connection == null) {
            connection = new Connection(dbAddress, port, dbName, userName, userPwd, userDb);
            if (!connection.open()) {
                return false;
            }
        }
        return true;
    }

    public static Connection getConnection() {
        return connection;
    }


    public static OperationWithUser getOperation(String name) {
        return operationMap.get(name);
    }
    public static OperationWithUser addOperation(
            String name,
            String itemName,
            String listName,
            int keyType
    ) {
        OperationWithUser op = new OperationWithUser(name,keyType,connection,itemName,listName);
        operationMap.put(name, op);
        return op;
    }

    public static OperationWithUser addOperation(OperationWithUser op) {
        operationMap.put(op.collectionName, op);
        return op;
    }

    public static int clearOperation() {
        int c = operationMap.size();
        operationMap.clear();
        return c;
    }

    public static String init(String dbAddress,int port, String dbName,String path){

        File settingsFile = new File(path);
        Runtimes.throwIf(!settingsFile.exists() || settingsFile.isDirectory(), "配置文件丢失");
        String settingString = Files.readStringFromFile(settingsFile);
        Runtimes.throwIf(Strings.isEmptyStrictly(settingString), "配置文件为空");
        Document platformDoc = Docat.json2doc(settingString);
        Runtimes.throwIf(platformDoc == null, "配置文件格式错误") ;

        return init(dbAddress,port,dbName,null,null,null,platformDoc);
    }

    public static String init(String dbAddress,int port, String dbName, String userName, String userPwd, String userDb, Document platformDoc) {

        String _dbAddress = Docat.getString(platformDoc, "dbAddress", "").trim();
        String _dbName = Docat.getString(platformDoc, "dbName", "").trim();

        if (_dbAddress.isEmpty()) {
            _dbAddress = dbAddress;
        }
        if (_dbName.isEmpty()) {
            _dbName = dbName;
        }

        // 先清除Cache
        D.trace("clearedCache: ", clearOperation());
        if (!conn(_dbAddress, port, _dbName, userName, userPwd, userDb)) {
            return "初始化连接失败";
        }


        Mongo.defaultKeyType = Docat.getInteger(platformDoc, "defaultKeyType", Mongo.defaultKeyType);
        Mongo.defaultFieldSpecs = Docat.getDocument(platformDoc, "defaultFieldSpecs", Mongo.defaultFieldSpecs);

        D.trace("Set default Key Type: ", Mongo.defaultKeyType);

        // 初始化必要表操作user表,file表
        OperationWithUser userOp = Mongo.addOperation(new UserOperation(Mongo.defaultKeyType, Mongo.getConnection()));
        OperationWithUser fileOp = Mongo.addOperation("file", "value", "values", Mongo.defaultKeyType);
        fileOp.addForeign(userOp, "user", new String[]{"_id", "name"}, true, true);
        fileOp.setPublic(true);

        Document collectionsDoc = platformDoc.get("collections", Document.class);
        if (collectionsDoc == null) {
            return "无法找到 collections 定义";
        }

        for (String name : collectionsDoc.keySet()) { // 初始化Cache列表
            Document doc = collectionsDoc.get(name, Document.class);
            String itemName = doc.getString("itemName");
            if (Strings.isEmptyStrictly(itemName)) {
                itemName = "value";
            }
            String listName = doc.getString("listName");
            if (Strings.isEmptyStrictly(listName)) {
                listName = "values";
            }
            int keyType = doc.getInteger("keyType", Mongo.defaultKeyType);
            Document fatherDoc = doc.get("father", Document.class);
            boolean hasParent = doc.getBoolean("hasParent", false);
            boolean isPublic = doc.getBoolean("isPublic", false);
            OperationWithUser operation = Mongo.addOperation(name, itemName, listName, keyType);
            if (fatherDoc != null) {
                String fatherName = fatherDoc.getString("name");
                // 判断 fatherName 是否符合输入
                if (fatherName != null && !fatherName.isEmpty() && Strings.trimAll(fatherName).equals(fatherName)) {
                    D.trace("/添加Fahter Cache: ", fatherName);
                    operation.setFather(fatherName, Mongo.getOperation(fatherName), fatherDoc.getBoolean("autoFill", false));
                }
            }
            operation.setHasParent(hasParent);
            operation.setPublic(isPublic);

            List<String> searchFields = doc.get("searchFields", List.class);

            if (searchFields != null && searchFields.size() > 0) {
                operation.setDefaultSearchFields(searchFields);
            }
            // 初始化字段定义，先初始化通用字段定义，再初始化集合专有字段定义
            operation.setFieldSpecs(Mongo.defaultFieldSpecs);
            operation.setFieldSpecs(Docat.getDocument(doc, "fieldSpecs"));

            D.trace("/添加Cache name:", name, " for collection: ", name);

            // 分别初始化foreign列表
            // 处理 foreigns
            Document foreignsDoc = doc.get("foreigns", Document.class);
            if (foreignsDoc != null) {
                for (String fname : foreignsDoc.keySet()) {
                    OperationWithUser foreignCache = Mongo.getOperation(fname);
                    if (foreignCache == null) {
                        D.trace("没找到 foreign cache(" + fname + ") 定义");
                        continue;
                    } else {
                        Object foreignDocOrList = foreignsDoc.get(fname);
                        List<Document> list;
                        if (foreignDocOrList instanceof Document) {
                            list = new ArrayList<Document>();
                            list.add((Document) foreignDocOrList);
                        } else {
                            list = (List) foreignDocOrList;
                        }
                        for (Document foreignDoc : list) {
                            String localAttr = foreignDoc.getString("localAttr").trim();
                            String[] foreignAttrs = Strings.trimAll(foreignDoc.getString("foreignAttrs")).split(",");
                            boolean forItem = foreignDoc.getBoolean("forItem", false);
                            boolean forList = foreignDoc.getBoolean("forList", false);
                            operation.addForeign(foreignCache, localAttr, foreignAttrs, forItem, forList);
                            D.trace("/添加Cache(", name, ")的foreign扩展 ", localAttr, " -> ", fname, "(", foreignDoc.getString("foreignAttrs").trim(), ")");
                        }
                    }
                }
            }

            // 处理lookup
            Document lookupsDoc = doc.get("lookups", Document.class);
            if (lookupsDoc != null) {
                for (String lname : lookupsDoc.keySet()) {
                    OperationWithUser lookupCache = Mongo.getOperation(lname);
                    if (lookupCache == null) {
                        D.trace("没找到 lookup cache(" + lname + ") 定义");
                        continue;
                    }
                    Object lookupDocOrList = lookupsDoc.get(lname);
                    List<Document> list;
                    if (lookupDocOrList instanceof Document) {
                        list = new ArrayList<Document>();
                        list.add((Document) lookupDocOrList);
                    } else {
                        list = (List) lookupDocOrList;
                    }

                    for (Document lookupDoc : list) {
                        String key = lookupDoc.getString("key").trim();
                        boolean isItem = !Docat.getBoolean(lookupDoc, "isList", false);
                        Object unsetValue = lookupDoc.get("unsetValue");
                        Object missedValue = lookupDoc.get("missedValue");
                        operation.addLookup(key, lookupCache, isItem, unsetValue, missedValue);
                        D.trace("/ ", name ," 添加 Lookup Cache: ", lname, ", key: ", key, ", isItem: ", isItem);
                    }
                }
            }

        }

        if(Mongo.operationMap.isEmpty()){
            return Key.ERR;
        }
        return Key.OK;

    }

}
