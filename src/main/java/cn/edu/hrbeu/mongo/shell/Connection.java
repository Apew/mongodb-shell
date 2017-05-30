package cn.edu.hrbeu.mongo.shell;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import cn.edu.hrbeu.mongo.shell.util.D;
import org.bson.Document;

import java.util.Arrays;

/**
 * Created by wu on 2017/5/25.
 */
public class Connection {

    private MongoClient mongo;
    private MongoDatabase db;
    //MongoDB数据库服务器所在地址
    private final String server;
    //端口
    private final int port;
    //数据库名
    private final String dbName;
    //用户名
    private final String userName;
    //用户密码
    private final String userPwd;
    //用户所在数据库
    private final String userDb;

    public Connection(String server, int port, String dbName, String userName, String userPwd, String userDb) {
        this.server = server;
        this.port = port;
        this.dbName = dbName;
        this.userName = userName;
        this.userPwd = userPwd;
        this.userDb = userDb;
    }

    public Connection(String server, int port, String dbName) {
        this(server, port, dbName, null, null, null);
    }

    public Connection(String server, String dbName) {
        this(server, 27017, dbName, null, null, null);
    }

    public Connection(String dbName) {
        this("localhost", 27017, dbName, null, null, null);
    }

    public boolean open() {
        try {
            if (this.userName != null) {
                MongoCredential credential = MongoCredential.createCredential(userName, userDb, userPwd.toCharArray());
                this.mongo = new MongoClient(new ServerAddress(this.server, this.port), Arrays.asList(credential));
            } else {
                this.mongo = new MongoClient(this.server, this.port);
            }

            for (String name : this.mongo.listDatabaseNames()) {
                if (name.equals(this.dbName)) {
                    D.trace("数据库 [", this.dbName, "] 存在，尝试打开...");
                    this.db = this.mongo.getDatabase(this.dbName);
                    break;
                }
            }

            if (this.db == null) {
                D.trace("数据库 [", this.dbName, "] 不存在存在，尝试新建...");
                this.db = this.mongo.getDatabase(this.dbName);
            }
            return true;
        } catch (MongoException ex) {
            ex.printStackTrace();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        D.trace("数据库 [", this.server, ":", this.port, "] 链接失败!");
        return false;
    }

    public MongoCollection<Document> getCollection(String collection) {
        //获取users DBCollection；如果默认没有创建，mongodb会自动创建
        return db.getCollection(collection);
    }

    public void close(){
        if (mongo != null) {
            mongo.close();
        }
        mongo = null;
        db = null;
        System.gc();
    }
}
