module process;

import std.datetime: Date;
import std.typecons: Tuple;
import dpq2: Connection;
import bookkeeping.titles: AccountValue;

void processTrial(Connection conn, AccountValue[string] values, in Tuple!(Date, "st", Date, "en") calcPeriod, in bool isMonthly){
  // queries
  import std.algorithm: countUntil;
  import dpq2;
  import query;
  import exception;

  int sale, fees, fare, insurance, insentive;
  /*******************************************
   * 通常の購買
   *
   * Table account_voucherから集計
   *******************************************/
  {
    string[] validTitles= values.keys;
    auto debits= cmdDebitOrCredit!"debit"(conn, calcPeriod, validTitles);
    auto credits= cmdDebitOrCredit!"credit"(conn, calcPeriod, validTitles);

    foreach(scope size_t i, string titleStr; validTitles){
      values[titleStr].priceDebit= debits[i];
      values[titleStr].priceCredit= credits[i];
    }
  }

  /*******************************************
   * 売上高
   *
   * 売上高= sum (1+税率)*sum(単価×量)
   *******************************************/
  sale= getSalePrice(conn, calcPeriod);
  values["製品売上高"].priceCredit += sale;

  /*******************************************
   * 各種販売手数料
   *
   * 市場手数料，農協手数料を集計
   *
   * 荷造運賃手数料の借方，売掛金の貸方へ記載
   ******************************************/
  {
    enum string QUERY_STR= `SELECT coalesce(sum(market_fee+ja_fee), 0) AS fee
FROM shipment_costs
WHERE shipment_costs.shipment_date >= $1::DATE
  AND shipment_costs.shipment_date < $2::DATE;`;

    QueryParams cmd;
    cmd.args.length= 2;
    cmd.sqlCommand= QUERY_STR;
    cmd.args[0]= toValue(calcPeriod.st.toISOExtString);
    cmd.args[1]= toValue(calcPeriod.en.toISOExtString);

    auto ansSQL= conn.execParams(cmd);
    fees= cast(int)(ansSQL[0]["fee"].as!long);
  }
  values["販売手数料"].priceDebit += fees;

  /*******************************************
   * 運賃
   *******************************************/
  {
    enum string QUERY_STR= `SELECT coalesce(sum(fare), 0) AS fare
FROM shipment_costs
WHERE shipment_costs.shipment_date >= $1::DATE
  AND shipment_costs.shipment_date < $2::DATE;`;

    QueryParams cmd;
    cmd.args.length= 2;
    cmd.sqlCommand= QUERY_STR;
    cmd.args[0]= toValue(calcPeriod.st.toISOExtString);
    cmd.args[1]= toValue(calcPeriod.en.toISOExtString);

    auto ansSQL= conn.execParams(cmd);
    fare= cast(int)(ansSQL[0]["fare"].as!long);
  }
  values["荷造運賃"].priceDebit += fare;

  /*******************************************
   * 保険負担金
   *
   * PL保険
   *
   * 保険料の借方，売掛金の貸方へ記載
   *******************************************/
  {
    enum string QUERY_STR= `SELECT coalesce(sum(insurance), 0) AS insurance
FROM shipment_costs
WHERE shipment_costs.shipment_date >= $1::DATE
  AND shipment_costs.shipment_date < $2::DATE;`;

    QueryParams cmd;
    with(cmd){
      cmd.args.length= 2;
      cmd.sqlCommand= QUERY_STR;
      cmd.args[0]= toValue(calcPeriod.st.toISOExtString);
      cmd.args[1]= toValue(calcPeriod.en.toISOExtString);
    }
    auto ansSQL= conn.execParams(cmd);
    insurance= cast(int)(ansSQL[0]["insurance"].as!long);
  }
  values["共済掛金"].priceDebit += insurance;

  /*****************************
   * 出荷奨励金
   *
   * 売掛金の借方，奨励金の貸方へ記載
   *****************************/
  {
    enum string QUERY_STR= `SELECT coalesce(sum(price), 0) AS insentive
FROM shipment_insentive
WHERE shipment_insentive.shipment_date >= $1::DATE
  AND shipment_insentive.shipment_date < $2::DATE;`;
    QueryParams cmd;
    with(cmd){
      cmd.args.length= 2;
      cmd.sqlCommand= QUERY_STR;
      cmd.args[0]= toValue(calcPeriod.st.toISOExtString);
      cmd.args[1]= toValue(calcPeriod.en.toISOExtString);
    }
    auto ansSQL= conn.execParams(cmd);
    insentive= cast(int)(ansSQL[0]["insentive"].as!long);
  }
  values["一般助成収入"].priceCredit += insentive;

  /*******************************************
   * 売掛金
   *
   * 売掛金の借方= 売上高-諸経費+奨励金
   *****************************************/
  values["売掛金"].priceDebit= sale-(fees+fare+insurance)+insentive;

  /*******************************************
   * 減価償却費
   *****************************************/
  {
    import bookkeeping.asset;

    const FixedAsset[] fixedAssets= () @system{
      import std.conv: to;
      enum string QUERY_STR= "SELECT * FROM fixed_assets;";
      auto answer= conn.exec(QUERY_STR);
      FixedAsset[] result= void;
      size_t idx= 0;

      if(!answer[0][0].isNull){
	if(answer[0].length == 6){
	  result= new FixedAsset[answer.length];	// GC
	  foreach(scope theRow; answer.rangify){
	    result[idx++]= FixedAsset(theRow["asset_name"].as!string,
				      Date.fromISOExtString(theRow["acquisition_date"].as!string),
				      theRow["title"].as!string,
				      to!uint(theRow["initial_price"].as!string),
				      to!uint(theRow["economic_life"].as!string));
	  }
	}
	else{
	  throw new TableStructNotCorrect("agriDB", "fixed_assets");
	}
      }
      else{}	// a null array returns

      return result;
    }();

    uint deprecPrice;
    foreach(scope theAsset; fixedAssets){
      if(isMonthly){
	deprecPrice= theAsset.priceDeprec!"month"(calcPeriod.st.year, calcPeriod.st.month);
      }
      else{
	deprecPrice= theAsset.priceDeprec!"year"(calcPeriod.st.year);
      }
      values["減価償却費"].priceDebit += deprecPrice;
      values[theAsset.accountTitle].priceCredit += deprecPrice;
    }
  }
}

/*************************************************************
 * 期間内の純利益を算出
 *************************************************************/
int getNetIncome(in AccountValue[string] values) @safe pure{
  import bookkeeping.titles: AccountCategory;
  int totalAsset, totalLiability, totalEquity, totalExpense, totalRevenue;
  foreach(scope theTitle; values){
    final switch(theTitle.category){
    case AccountCategory.asset:
      totalAsset += theTitle.balanceThisTerm;
      break;
    case AccountCategory.liability:
      totalLiability += theTitle.balanceThisTerm;
      break;
    case AccountCategory.equity:
      totalEquity += theTitle.balanceThisTerm;
      break;
    case AccountCategory.expense:
      totalExpense += theTitle.balanceThisTerm;
      break;
    case AccountCategory.revenue:
      totalRevenue += theTitle.balanceThisTerm;
    }
  }

  const int incomesBS= totalAsset-totalLiability-totalEquity;
  const int incomesPL= totalRevenue-totalExpense;
  if(incomesBS != incomesPL){
    throw new Exception("貸借対照表と損益計算表の結果が不一致");
  }

  return incomesBS;
}
