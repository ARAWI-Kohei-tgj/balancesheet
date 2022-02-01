// database
enum DataBaseAccess{
  readonly,	// SELECT
  append,	// INSERT, UPDATE
  delete_	// DELETE
}

/*************************************************************
 * 存在check
 *************************************************************/
import dpq2: Connection;
bool isExist(Connection conn, in string tableName, in string condition) @system{
  import dpq2: Connection, QueryParams;
  @(DataBaseAccess.readonly) QueryParams cmd;
  with(cmd){
    import std.array: appender;
    import std.format: formattedWrite;
    
    auto buf= appender!string;
    buf.formattedWrite!"SELECT * FROM %s WHERE %s;"(tableName, condition);
    sqlCommand= buf.data;
  }
  auto ansSQL= conn.execParams(cmd);
  return ansSQL.length > 0? true: false;
}

// 動作
enum ActionMode{trial, close}
enum OutputFormat{terminal, csv, libreOffice}

/*****************************************************************************
 *
 * -mode= [trial, close]
 *  default is `trial'.
 *
 * -month=YYYY-MM
 *  1ヶ月単位の計算
 *
 * -month= and -mode=close は不可
 *
 * -output= [terminal, csv, libreoffice]
 *****************************************************************************/
void main(in string[] argMain) @system{
  import std.stdio;
  import std.datetime;
  import std.typecons: Tuple, tuple;
  import dpq2;
  import bookkeeping.titles;
  import exception;

  // 期首と期末
  immutable Month periodStartMonth= Month.jan;
  immutable Month periodEndMonth= Month.dec;

  /**********************************************************
   * start-up process
   **********************************************************/
  // TEMP:
  if(argMain.length < 2 || argMain.length > 3){
    throw new Exception("Error: Invalid arguments.");
  }

  bool isMonthly;	// TEMP:
  string[3] spcStr;

  {
    size_t idx;
    foreach(scope str; argMain[1..$]){
      switch(str[0..6]){
      case "-mode=":
        idx= 0;
	break;
      case "-outpu":
	idx= 1;
	break;
      case "-month":
	idx= 2;
	break;
      default:
	throw new Exception("Error: invalid option.");
      }
      spcStr[idx]= str;
    }
  }

  // mode
  const ActionMode mode= (in string str){// @safe pure
    ActionMode result;
    if(str.length == 0) result= ActionMode.trial;
    else{
      import std.algorithm: findSplit;
      auto tokens= str.findSplit("=");
      switch(tokens[2]){
      case "trial":
	result= ActionMode.trial;
	break;
      case "close":
	result= ActionMode.close;
	break;
      default:
	throw new Exception("Error: Invalid mode is speciied.");
      }
    }
    return result;
  }(spcStr[0]);

  const OutputFormat outFormat= OutputFormat.terminal;

  // time period of the calcuration
  const Tuple!(Date, "st", Date, "en") calcPeriod= (in string str) @safe pure{
    import std.algorithm: findSplit;
    import std.conv: to;
    Date[2] result;

    auto tokens= str.findSplit("=");
    string lhs= tokens[0];
    if(lhs == "-month"){
      const int rhsYear= to!int(tokens[2][0..4]);
      const int rhsMonth= to!int(tokens[2][5..7]);
      result[0]= Date(rhsYear, rhsMonth, 1);
      switch(rhsMonth){
      case 1: .. case 11:
	result[1]= Date(rhsYear, rhsMonth+1, 1);
	break;
      case 12:
	result[1]= Date(rhsYear+1, 1, 1);
	break;
      default:
	throw new Exception("Error: Invalid month is speciied.");
      }
      isMonthly= true;
    }
    else{
      throw new Exception("Error: Invalid argument `" ~lhs ~"'.");
    }
    return tuple!("st", "en")(result[0], result[1]);
  }(spcStr[2]);

  // DB connection
  Connection toAgriDB= (in string loginID, in string password) @system{
    import std.format: format;

    enum string FORMAT_STR= "host=%s port=%s dbname=%s user=%s password=%s";
    enum string host_address= "localhost";
    enum string db_name= "agridb";
    enum string port= "5432";
    immutable  string str= format!FORMAT_STR(host_address, port, db_name, loginID, password);

    return new Connection(str);
  }("arawi_kohei", "2sc1815_2sa1015_");

  /***********************************************************
   * main process
   ***********************************************************/
  // container
  AccountValue[string] values= () @safe pure nothrow{
    AccountValue[string] result;
    string[] temp= accountTitles;
    temp ~= "前期純利益";

    foreach(scope titleStr; temp){
      result[titleStr]= AccountValue(titleStr);}
    return result;
  }();

  /*******************************************
   * Table `account_voucher' から入力
   *******************************************/
  if(mode is ActionMode.trial){
    import process: processTrial;
    processTrial(toAgriDB, values, calcPeriod, isMonthly);
  }
  else{}
    
  /***********************************************************
   * 前月の集計結果をDBから読み込み
   *   isMonthly = true の場合
   *     calcPeriod.stが1月1日 -> balance_closedから読み込み
   *     calcPeriod.stが1月1日でない -> balance_trialから読み込み
   *   isMonthly = false の場合 -> balance_closedの前年12月末から読み込み
   ***********************************************************/
  (Connection conn) @system{
    import std.algorithm: canFind;

    // 参照先（前月 or 前年）のtable名
    const string tableNameRef= (calcPeriod.st.month is periodStartMonth)? "balance_closed": "balance_trial";

    const Date lastClosing= ((mode is ActionMode.trial)?
      calcPeriod.st: calcPeriod.en) -dur!"days"(1);
    QueryParams cmd;
    string titleName;
    {
      import std.array: appender;
      import std.format: formattedWrite;

      enum string QUERY_STR= `SELECT title, balance
FROM %s
WHERE closing_date = $1::DATE;`;
      auto buf= appender!string;
      buf.formattedWrite!QUERY_STR(tableNameRef);
      cmd.sqlCommand= buf.data;
    }

    cmd.args.length= 1;
    cmd.args[0]= toValue(lastClosing.toISOExtString);
    @(DataBaseAccess.readonly) auto result= conn.execParams(cmd);

    if(result.length == 0){
      throw new Exception("Error: 前月の集計結果が未登録 in Table `" ~tableNameRef ~"'．");
    }

    foreach(scope row; result.rangify){
      titleName= row["title"].as!string;

      final switch(mode){
      case ActionMode.trial:
	if(calcPeriod.st.month is periodStartMonth){	// 期首
	  final switch(values[titleName].category){
	  case AccountCategory.asset, AccountCategory.liability, AccountCategory.equity:
	    values[titleName].balanceLast= row["balance"].as!int;
	    break;
	  case AccountCategory.expense, AccountCategory.revenue:
	    values[titleName].balanceLast= 0;
	  }
	}
	else{
	  values[titleName].balanceLast= row["balance"].as!int;	// 前月残高へ代入
	}
	break;
      case ActionMode.close:
	final switch(values[titleName].category){	// 借方または貸方へ代入
	case AccountCategory.asset, AccountCategory.expense:
	  values[titleName].priceDebit= row["balance"].as!int;
	  break;
	case AccountCategory.liability, AccountCategory.equity, AccountCategory.revenue:
	  values[titleName].priceCredit= row["balance"].as!int;
	}
      }
    }
  }(toAgriDB);
/+
  foreach(scope key; accountTitles){	// DEBUG:
    writefln!"%s: %d, %d"(key,
			  values[key].priceDebit,
			  values[key].priceCredit);
  }
+/

  /***********************************************************
   * 期首処理および期末処理
   *
   * 期首
   * (1) 元入金の更新[tag= CAPITAL_UPGRADING]
   *    今期首元入金= 前期末元入金+事業主借-事業主貸+純利益
   * (2) 前期の期末材料棚卸高を今期の期首材料棚卸高へ振替
   *
   * 期末
   * (1) 預金利息の振替[tag= DEPO_INTEREST_TRANSFER]
   * 1.0 預金利息の処理は下記の状態になってゐる
   * title_debit= 普通預金, price= xxx
   * title_credit= 受取利息, price= xxx
   * 1.1 二重課税を回避するため，Table `account_process'に下記の振替処理を追加
   * title_debit= 受取利息, price= xxx
   * title_credit= 事業主借, price= xxx
   *
   * (2) 期末棚卸し処理[tag= INVENTORY_TRANSFER]
   * 2.1 Table `inventory' を参照
   * 2.2 Table `accont_process' に振替処理を追加
   ***********************************************************/
  if((mode is ActionMode.trial && calcPeriod.st.month is periodStartMonth) ||
     mode is ActionMode.close){
    @(DataBaseAccess.append) QueryParams procTerminal;
    with(procTerminal){
      sqlCommand= `INSERT INTO account_process
(proc_date, summary, price, title_debit, title_credit) VALUES
($1::DATE, $2::TEXT, $3::INTEGER, $4::TEXT, $5::TEXT);`;
      args.length= 5;
    }

    final switch(mode){
    case ActionMode.trial:	// 期首処理
    CAPITAL_UPGRADING:
      // debit=元入金, credit=事業主貸 
      with(procTerminal){
	args[0]= toValue(calcPeriod.st.toISOExtString);
	args[1]= toValue("元入金への振替");
	args[2]= toValue(values["事業主貸"].balanceLast);
	args[3]= toValue("元入金");
	args[4]= toValue("事業主貸");
      }
      toAgriDB.execParams(procTerminal);

      // debit=事業主借, credit=元入金
      with(procTerminal){
	args[0]= toValue(calcPeriod.st.toISOExtString);
	args[1]= toValue("元入金への振替");
	args[2]= toValue(values["事業主借"].balanceLast);
	args[3]= toValue("事業主借");
	args[4]= toValue("元入金");
      }
      toAgriDB.execParams(procTerminal);

      // 純利益 -> 元入金
      const int profitLast= (Connection conn){
	@(DataBaseAccess.readonly) QueryParams getLastProfit;
	with(getLastProfit){
	  Date dateLastClosing= calcPeriod.st-dur!"days"(1);
	  sqlCommand= `SELECT price FROM net_income_closed WHERE closing_date = $1::DATE;`;
	  args.length= 1;
	  args[0]= toValue(dateLastClosing.toISOExtString);
	}
	auto ansSQL= conn.execParams(getLastProfit);
	return ansSQL[0]["price"].as!int;
      }(toAgriDB);

      with(procTerminal){
	args[0]= toValue(calcPeriod.st.toISOExtString);
	args[1]= toValue("元入金への振替");
	args[2]= toValue(profitLast);
	args[3]= toValue("前期純利益");
	args[4]= toValue("元入金");
      }
      toAgriDB.execParams(procTerminal);

      values["元入金"].balanceLast += values["事業主借"].balanceLast-values["事業主貸"].balanceLast+profitLast;
      values["事業主貸"].balanceLast= 0;
      values["事業主借"].balanceLast= 0;

    INVENTORY_TRANSFER_BEG:
      const string[4] ingredient= () @safe pure nothrow{
	import std.algorithm: canFind;
	const string[4] result= ["出荷資材", "諸材料", "肥糧", "その他原材料"];
	foreach(scope theTitle; result) assert(accountTitles[].canFind(theTitle));
	return result;
      }();
      foreach(theTitle; ingredient){
	if(scope priceTemp= values[theTitle].balanceLast){
	  values["期首材料棚卸高"].balanceLast += priceTemp;
	  values[theTitle].balanceLast= 0;

	  with(procTerminal){
	    args[0]= toValue(calcPeriod.st.toISOExtString);
	    args[1]= toValue("期首棚卸し処理");
	    args[2]= toValue(priceTemp);
	    args[3]= toValue("期首材料棚卸高");
	    args[4]= toValue(theTitle);
	  }
	  toAgriDB.execParams(procTerminal);
	}
	else continue;
      }
      break;

    case ActionMode.close:	// 期末処理
      const Date closingDate= calcPeriod.en -dur!"days"(1);

    DEPO_INTEREST_TRANSFER:
      (Connection conn) @system{
	const int thePrice= values["受取利息"].priceCredit;
	with(procTerminal){
	  args[0]= toValue(closingDate.toISOExtString);
	  args[1]= toValue("預金利息の振替処理");
	  args[2]= toValue(thePrice);
	  args[3]= toValue("受取利息");
	  args[4]= toValue("事業主借");
	}
	conn.execParams(procTerminal);	// writing
      }(toAgriDB);

    INVENTORY_TRANSFER_TERMINAL:
      (Connection conn) @system{
	enum string QUERY_STR= `SELECT summary,
  unit_price*amount AS price,
  title
FROM inventory
WHERE survey_date = $1::DATE;`;
	@(DataBaseAccess.readonly) QueryParams cmd;
	with(cmd){
	  sqlCommand= QUERY_STR;
	  args.length= 1;
	  args[0]= toValue(closingDate.toISOExtString);
	}
	auto ansSQL= conn.execParams(cmd);	// reading

	foreach(scope row; ansSQL.rangify){
	  with(procTerminal){
	    args[0]= toValue(closingDate.toISOExtString);
	    args[1]= toValue("期末棚卸し処理");
	    args[2]= row["price"];
	    args[3]= row["title"];
	    args[4]= toValue("期末材料棚卸高");
	  }
	  conn.execParams(procTerminal);	// writing
	}
      }(toAgriDB);

      /***********************
       * Table `account_process' から期末の特殊処理を取得
       *
       * 上記の2つの処理はDBを経由せず直接入力することもできるが，
       * このプログラム以外からDBに直接入力される場合を想定し，DB経由で取得する．
       ***********************/
      (Connection conn) @system{
	enum string QUERY_STR= `SELECT price, title_debit, title_credit
FROM account_process
WHERE proc_date = $1::DATE;`;
	const Date processDate= (calcPeriod.st.month is periodStartMonth)? calcPeriod.st: calcPeriod.en -dur!"days"(1);
	@(DataBaseAccess.readonly) QueryParams cmd;
	with(cmd){
	  cmd.sqlCommand= QUERY_STR;
	  cmd.args.length= 1;
	  cmd.args[0]= toValue(processDate.toISOExtString);
	}
	auto ansSQL= conn.execParams(cmd);
	foreach(scope row; ansSQL.rangify){
	  values[row["title_debit"].as!string].priceDebit += row["price"].as!int;
	  values[row["title_credit"].as!string].priceCredit += row["price"].as!int;
	}
      }(toAgriDB);
    }	// end of the final_switch statement
  }	// end of the if statement
/+
  foreach(scope key; values.byKey){	// DEBUG:
    writefln!"%s: %d, %d"(key,
			  values[key].priceDebit,
			  values[key].priceCredit);
  }
+/

  /*******************************************
   * 集計内容をDBに登録
   *
   * (1) isTrial = true の場合
   * Table `balance_trial' に登録
   *  title TEXT PRIMARY KEY = 勘定科目
   *  closing_date DATE NOT NULL = 決算日時
   *  balance INTEGER NOT NULL = 残高
   *
   * (2) isTrial = false の場合
   * Table `balance_closed' に登録
   *******************************************/
  {
    // 登録先のtable名
    const string tableNameRgstr= (mode is ActionMode.trial)? "balance_trial": "balance_closed";
    const Date closingDate= calcPeriod.en -dur!"days"(1);

    // 既に登録されてゐないか検査
    const bool alreadyExist= isExist(toAgriDB,
				     tableNameRgstr,
				     "closing_date = to_date('" ~closingDate.toISOExtString ~"', 'YYYY-MM-DD')");

    // 既に登録されてゐる場合に上書きするか？
    const bool writingAllowed= (in bool isExist) @system{
      import std.stdio: writefln, write, readln;

      bool result;
      if(isExist){
	writefln!"NOTICE: Balance data at %s have already registered."(closingDate);
        write("CHOICE: Overwrite Y/N ?: ");
	const string answerStr= readln();
	if(answerStr !is null && (answerStr[0] == 'Y' ||
				  answerStr[0] == 'y')) result= true;
	else result= false;
      }
      else result= true;

      return result;
    }(alreadyExist);

    if(writingAllowed){
      // 既に登録されてゐる内容を削除
      if(alreadyExist){
	(Connection conn){
	  enum string QUERY_STR= `DELETE FROM $1::TEXT
WHERE closing_date >= $2::DATE
  AND closing_date < $3::DATE;`;
	  @(DataBaseAccess.delete_) QueryParams cmd;
	  with(cmd){
	    sqlCommand= QUERY_STR;
	    args.length= 3;
	    args[0]= toValue(tableNameRgstr);
	    args[1]= toValue(calcPeriod.st.toISOExtString);
	    args[2]= toValue(calcPeriod.en.toISOExtString);
	  }
	  conn.execParams(cmd);
	}(toAgriDB);
      }

      // DBへ出力
      (Connection conn, in Date theDate) @system{
	enum string QUERY_TRIAL= `INSERT INTO balance_trial
(title, closing_date, balance) VALUES
($1::TEXT, $2::DATE, $3::INTEGER);`;
	enum string QUERY_CLOSED= `INSERT INTO balance_closed
(title, closing_date, balance) VALUES
($1::TEXT, $2::DATE, $3::INTEGER);`;

	@(DataBaseAccess.append) QueryParams cmdExistance, cmdReg;
	size_t idx= 1;

	cmdReg.sqlCommand= (mode is ActionMode.trial)? QUERY_TRIAL: QUERY_CLOSED;
	cmdReg.args.length= 3;
	cmdReg.args[1]= toValue(theDate.toISOExtString);

	foreach(scope titleStr; accountTitles){
	  cmdReg.args[0]= toValue(titleStr);
	  cmdReg.args[2]= toValue(values[titleStr].balanceTotal);
	  conn.execParams(cmdReg);
	}
	writefln!"NOTICE: registration successed";
      }(toAgriDB, closingDate);
    }
    else{
      writefln!"NOTICE: registrations are canceled."();
    }
  }

  /*******************************************
   * 純利益をDBに登録
   *******************************************/
  {
    import std.array: appender;
    import std.format: formattedWrite;
    import process: getNetIncome;

    const string tableNameRgstr= (mode is ActionMode.trial)?
      "net_income_monthly": "net_income_closed";
    const Date closingDate= calcPeriod.en -dur!"days"(1);
    const price= getNetIncome(values);

    const bool alreadyExist= toAgriDB.isExist(tableNameRgstr,
					      "closing_date = to_date('"
					        ~closingDate.toISOExtString
					        ~"', 'YYYY-MM-DD')");

    // 既に登録されてゐる場合に上書きするか？
    const bool writingAllowed= (Connection conn, in bool isExist) @system{
      import std.stdio: writefln, write, readln;

      bool result;
      if(isExist){
	writefln!"NOTICE: income data at %s have already registered."(closingDate);
        write("CHOICE: Overwrite Y/N ?: ");
	const string answerStr= readln();
	if(answerStr !is null && (answerStr[0] == 'Y' ||
				  answerStr[0] == 'y')) result= true;
	else result= false;
      }
      else result= true;

      return result;
    }(toAgriDB, alreadyExist);

    if(writingAllowed){
      if(alreadyExist){
	(Connection conn){
	  @(DataBaseAccess.readonly) QueryParams cmd;
	  with(cmd){
	    sqlCommand= "DELETE FROM " ~tableNameRgstr ~" WHERE closing_date = $1::DATE;";
	    args.length= 1;
	    args[0]= toValue(closingDate);
	  }
	  conn.execParams(cmd);
	}(toAgriDB);
      }

      (Connection conn){
	enum string QUERY_STR= `INSERT INTO %s
(closing_date, price) VALUES
($1::DATE, $2::INTEGER);`;
	auto buf= appender!string;
	@(DataBaseAccess.append) QueryParams cmd;
	buf.formattedWrite!QUERY_STR(tableNameRgstr);
	with(cmd){
	  sqlCommand= buf.data;
	  args.length= 2;
	  args[0]= toValue(closingDate.toISOExtString);
	  args[1]= toValue(price);
	}
	conn.execParams(cmd);
	writefln!"Inclemental net profit= %d"(price); 
      }(toAgriDB);
    }
  }

  // output
  {
    import std.array: array;
    import std.range: repeat;
    import std.conv: dtext;
    writeln("勘定科目:\tDebit\tCredit");
    foreach(scope titleStr; accountTitles){
      writefln!"%s%s:\t%7d\t%7d"(titleStr,
				 '　'.repeat(12-dtext(titleStr).length).array,
				 values[titleStr].priceDebit,
				 values[titleStr].priceCredit);
    }
  }
}
