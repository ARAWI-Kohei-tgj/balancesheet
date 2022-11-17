module titles;

/*************************************************************
 * 勘定科目の区分
 *************************************************************/
enum AccountCategory{asset,	// 資産
		     liability,	// 負債
		     equity,	// 資本
		     revenue,	// 収益
		     expense}	// 費用

/*************************************************************
 * 勘定科目の一覧
 *************************************************************/
immutable string[61] titlesAsset;
immutable string[12] titlesLiability;
immutable string[3] titlesEquity;
immutable string[23] titlesRevenue;
immutable string[61] titlesExpense;
immutable string[4] inventoryTitles;
string[] accountTitles() @safe pure nothrow @property{
  string[] buf;
  buf.reserve(titlesAsset.length
	      +titlesLiability.length
	      +titlesEquity.length
	      +titlesRevenue.length
	      +titlesExpense.length);
  buf ~= titlesAsset[] ~titlesLiability[]
    ~titlesEquity[] ~titlesRevenue[] ~titlesExpense[]
    ~"前期純利益";	// TEMP:
  return buf;
};

/*************************************************************
 * 仕訳データ
 *************************************************************/
struct AccountValue{
  this(in string title) @safe pure nothrow @nogc{
    import std.algorithm: canFind;
    if(titlesAsset[].canFind(title)){
      _category= AccountCategory.asset;
    }
    else if(titlesLiability[].canFind(title)){
      _category= AccountCategory.equity;
    }
    else if(titlesEquity[].canFind(title)){
      _category= AccountCategory.equity;
    }
    else if(titlesRevenue[].canFind(title)){
      _category= AccountCategory.revenue;
    }
    else if(titlesExpense[].canFind(title)){
      _category= AccountCategory.expense;
    }
    else{
      //assert(false);	// TEMP:
    }

    _title= title;
    priceDebit= 0;
    priceCredit= 0;
  }

  int balanceLast;
  int priceDebit;
  int priceCredit;

  @safe pure const{
    /*****************************************
     * 勘定科目
     *****************************************/
    string title() nothrow @nogc{
      return _title;
    }

    /*****************************************
     * 勘定科目の区分
     *****************************************/
    AccountCategory category() nothrow @nogc{
      return _category;
    }

    /*****************************************
     * 期間内の決算
     *****************************************/
    int balanceThisTerm() nothrow @nogc{
      typeof(return) result= 0;
      final switch(_category){
      case AccountCategory.asset, AccountCategory.expense:
	result= priceDebit-priceCredit;
	break;
      case AccountCategory.liability, AccountCategory.equity, AccountCategory.revenue:
	result= -priceDebit+priceCredit;
      }

      return result;
    }

    /*****************************************
     * 累計の決算
     *****************************************/
    int balanceTotal() nothrow @nogc{
      return balanceThisTerm +balanceLast;
    }
  }

private:
  string _title;
  AccountCategory _category;
}

shared static this(){
  import std.algorithm: canFind;
  titlesAsset= [
    "現金", "当座預金", "普通預金（JAバンク）", "定期預金",
    "定期積立", "その他預金", "受取手形", "売掛金",
    "貸倒引当金", "有価証券", "製品", "出荷資材",
    "諸材料", "肥糧", "その他原材料", "仕掛品",
    "貯蔵品", "前渡金", "前払費用", "未収消費税等",
    "短期貸付金", "未収入金", "預け金", "立替金",
    "仮払金", "建物", "建物付属設備", "構築物",
    "機械装置", "車輌運搬具", "器具備品", "生物",
    "繰延生物", "一括償却資産", "土地", "建設仮勘定",
    "育成仮勘定", "商標権", "実用新案権", "意匠権",
    "育成者権", "ソフトウェア", "土地改良負担金", "借家権",
    "借地権", "電話加入権", "投資有価証券", "関係会社株式",
    "出資金", "関係会社出資金", "長期貸付金", "破産等債権",
    "長期前払費用", "客土", "保険積立金", "経営保険積立金",
    "長期預け金", "開業費", "開発費", "事業主貸",
    "未収穫農産物"];
  titlesLiability= [
    "買掛金", "短期借入金", "未払金", "未払費用",
    "未払消費税等", "前受金", "預り金", "仮受金",
    "長期借入金", "長期未払金", "農業経営基盤強化準備金", "事業主借"];
  titlesEquity= ["元入金", "資本金", "株主資本"];
  titlesRevenue= [
    "製品売上高", "生物売却収入", "作業受託収入", "価格補填収入",
    "生物売却原価", "事業消費高", "受取利息", "受取配当金",
    "受取地代家賃", "受取共済金", "一般助成収入", "作付助成収入",
    "飼糧補填収入", "雑収入", "固定資産売却益", "投資有価証券売却益",
    "経営安定補填収入", "収入保険補填収入", "保険差益", "償却債権取立益",
    "貸倒引当金戻入額",
    "期首商品製品棚卸高", "期末商品製品棚卸高"];
  titlesExpense= [
    "荷造運賃", "販売手数料", "広告宣伝費", "交際費",	// 費用[100..160]
    "旅費交通費", "事務通信費", "車輌費", "図書研修費",
    "支払報酬", "支払手数料", "減価償却費", "開発費償却",
    "地代家賃", "支払保険料", "諸会費", "寄附金",
    "貸倒引当金繰入額", "雑費", "種苗費", "素畜費",
    "肥糧費", "飼糧費", "農薬費", "敷料費",
    "燃油費", "諸材料費", "賃金手当", "作業用衣料費",
    "作業委託費", "診療衛生費", "預託費", "ヘルパー利用費",
    "圃場管理費", "農具費", "修繕費", "動力光熱費",
    "共済掛金", "とも補償拠出金", "農地賃借料", "地代賃借料",
    "土地改良費", "租税公課（一般管理費分）", "租税公課（製造経費分）", "支払利息",
    "手形譲渡損", "廃畜処分損", "雑損失", "固定資産売却損",
    "固定資産除却損", "固定資産圧縮損", "災害損失", "特別償却費",
    "給料手当", "雑給", "賞与", "退職金",
    "退職給付費用", "法定福利費", "福利厚生費",
    "期首材料棚卸高", "期末材料棚卸高"];

  inventoryTitles= ["期首商品製品棚卸高", "期末商品製品棚卸高",
		    "期首材料棚卸高", "期末材料棚卸高"];
  foreach(scope theTitle; inventoryTitles) assert(accountTitles[].canFind(theTitle), "mismatch inventory title.");
}
