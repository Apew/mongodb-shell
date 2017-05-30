package cn.edu.hrbeu.mongo.shell;

/**
 * Created by wu on 2017/5/26.
 */
public class Key {
    public static final String HAS_LOGINED = "HAS_LOGINED";
    public static final String NOT_LOGINED = "NOT_LOGINED";
    public static final String NO_AUTHORITY = "NO_AUTHORITY";

    public static final int EC_NO_AUTHORITY = 10;
    public static final String ES_NO_AUTHORITY = "NO_AUTHORITY";
    public static final int EC_BAD_FORMAT = 11;
    public static final String ES_BAD_FORMAT = "BAD_FORMAT";
    public static final int EC_UNSUPPORT_OPERATION = 12;
    public static final String ES_UNSUPPORT_OPERATION = "UNSUPPORT_OPERATION";

    public static final String TAG_TAG = "TAG";
    public static final String OPERATION_TAG = "OP";
    public static final String TARGET_TABLE_TAG = "TT"; // 目标表
    public static final String RETURN_CODE_TAG = "RC";
    public static final String RETURN_REASON_TAG = "WHY";

    public static final int ROLE_ADMIN = 9;

    public static final String OK = "OK";
    public static final String ERR = "ERR";

    static public final int _ASC = 1;
    static public final int _DESC = -1;
    static public final String _CREATE_TIME = "createTime";
    static public final String _UPDATE_TIME = "updateTime";
    static public final String _FINISH_TIME = "finishTime";
    static public final String _ID = "_id";
    static public final String ID = "id";
    static public final String _PID = "_pid";
    static public final String PID = "pid";
    static public final String _FID = "_fid";
    static public final String FID = "fid";

    static public final String _USER = "user";

    static public final String _AUTH_ROOT = "_auth";
    static public final int _AUTH_NONE = 0;
    static public final int _AUTH_READ = 1;
    static public final int _AUTH_WRITE = 2;

    static public final int _INCLUDE = 1;
    static public final int _EXCLUDE = 0;

    static public final boolean _WITH_TOTAL = true;
}
