module exception;

/***********************************************************
 *
 ***********************************************************/
class DataBaseError: Exception{
  this(in string dbName, in string argStr){
    import std.array: appender;
    import std.format: formattedWrite;

    enum string MSG_STR= "DB error: in DB %s, %s.";
    auto buf= appender!string;

    buf.formattedWrite!MSG_STR(dbName, argStr);
    super(buf.data);
  }
}

/*********************************************
 * DB error: table `hoge' does not exist.
 *********************************************/
class TableNotExist: DataBaseError{
  this(in string dbName, in string tableName){
    super(dbName, "table `" ~tableName ~"' does not exist.");
  }
}

/*********************************************
 *
 *
 * table `hoge'
 *********************************************/
class TableStructNotCorrect: DataBaseError{
  this(in string dbName, in string tableName){
    super(dbName, "structure of table `" ~tableName ~"' is mismatched.");
  }
}
