package cn.edu.hrbeu.mongo.shell;

import org.bson.Document;

/**
 * Created by wu on 2017/5/26.
 */
public class DB {
    public static OperationWithUser user;
    public static OperationWithUser file;
    static void init(){
        String address = "localhost";
        int port = 27017;
        String database = "test";
        String path = "conf/mongo.conf";
        Mongo.init(address,port,database,path);
        user = Mongo.getOperation("user");
        file = Mongo.getOperation("file");
    }

    public static void main(String[] args) {
        DB.init();
        DB.user.__insert_one(null,new Document("name","wuxiang"),System.currentTimeMillis());
    }
}
