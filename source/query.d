/*****************************************************************************
 * Query
 *****************************************************************************/
module query;

import std.datetime: Date;
import std.typecons: Tuple;
import dpq2: Connection;
import bookkeeping.titles: accountTitles;

/***********************************************************
 * 通常の購買
 ***********************************************************/
int[] cmdDebitOrCredit(string Mode)(Connection conn,
				    in Tuple!(Date, "st", Date, "en") period,
				    in string[] validTitles) @system
if(Mode == "debit" || Mode == "credit"){
  import std.algorithm: countUntil;
  import dpq2;import std.stdio;

  /***************************
   * 各勘定科目ごとの借方の金額を集計
   ***************************/
  enum string QUERY_DEBIT= `SELECT title_debit, coalesce(sum(price), 0)
FROM account_voucher
WHERE tr_date >= to_date($1::TEXT, 'YYYY-MM-DD')
  AND tr_date < to_date($2::TEXT, 'YYYY-MM-DD')
GROUP BY title_debit;`;

  /***************************
   * 各勘定科目ごとの貸方の金額を集計
   ***************************/
  enum string QUERY_CREDIT= `SELECT title_credit, coalesce(sum(price), 0)
FROM account_voucher
WHERE tr_date >= to_date($1::TEXT, 'YYYY-MM-DD')
  AND tr_date < to_date($2::TEXT, 'YYYY-MM-DD')
GROUP BY title_credit;`;

  QueryParams cmd;
  int[] result= new int[validTitles.length];
  result[]= 0;

  with(cmd){
    args.length= 2;
    static if(Mode == "debit"){
      sqlCommand= QUERY_DEBIT;
    }
    else{
      sqlCommand= QUERY_CREDIT;
    }
    args[0]= toValue(period.st.toISOExtString);
    args[1]= toValue(period.en.toISOExtString);
  }

  auto ansSQL= conn.execParams(cmd);
  ptrdiff_t titleIdx;
  string titleName;

  foreach(scope row; ansSQL.rangify){
    titleName= row[0].as!string;
    titleIdx= validTitles[].countUntil(titleName);
    if(titleIdx >= 0){
      result[titleIdx]= cast(int)(row[1].as!long);
    }
    else{
      throw new Exception("Error: Invalid account title `" ~titleName ~"' is found.");
    }
  }

  return result;
}

/***********************************************************
 * 売掛金
 ***********************************************************/
int getSalePrice(Connection conn, in Tuple!(Date, "st", Date, "en") period) @system{
  import dpq2;

  /***************************
   * 売掛金の集計
   *
   * 出荷物の総売上金額を集計
   * 以下の手順で集計する
   * (1) ある作物について，等級・階級別にfloor(単価×数量)を計算
   * (2) (1)の総和を求める
   * (3) (2)は税抜き価格であるため，消費税込みの価格を算出
   * (4) 指定期間内で(3)の総和を求める
   * (5) 指定期間内に出荷履歴のある全作物に亘り(4)の総和を求める
   *
   * 売掛金の借方，売上高の貸方へ記載
   ***************************/
  enum string QUERY_TEMPL= `WITH quantity_and_price AS(
  SELECT shipment_quantity.shipment_date AS "shipment_date",
    shipment_quantity.crop_name AS "crop_name",
    shipment_quantity.class_ AS "class_",
    shipment_quantity.nominal_mass AS "nominal_mass",
    sale_price.unit_price AS "unit_price",
    sale_price.unit_mass AS "unit_mass"
  FROM shipment_quantity INNER JOIN sale_price
    ON shipment_quantity.crop_name = sale_price.crop_name
    AND shipment_quantity.class_ = sale_price.class_
    AND shipment_quantity.shipment_date = sale_price.shipment_date
)
SELECT COALESCE(CAST(ROUND(1.08*SUM(FLOOR(unit_price*nominal_mass*1000/unit_mass))) AS INTEGER)) AS "sale_daily"
FROM quantity_and_price
WHERE quantity_and_price.shipment_date >= to_date($1::TEXT, 'YYYY-MM-DD')
  AND quantity_and_price.shipment_date < to_date($2::TEXT, 'YYYY-MM-DD')
GROUP BY crop_name, shipment_date;
`;
  //CAST(ROUND(1.08*SUM(FLOOR(unit_price*COALESCE(nominal_mass, 0.0)*1000/unit_mass))) AS INTEGER)
  QueryParams cmd;
  typeof(return) result= 0;

  with(cmd){
    args.length= 2;
    sqlCommand= QUERY_TEMPL;
    args[0]= toValue(period.st.toISOExtString);
    args[1]= toValue(period.en.toISOExtString);
  }
  auto ansSQL= conn.execParams(cmd);

  foreach(scope row; ansSQL.rangify) result += row[0].as!int;
  return result;

  /**
   * 検算
   *
   * 貸借対照表等式: sum(資産) = sum(負債)+sum(資本)
   * 損益計算書等式: sum(費用)+sum(純利益)= sum(収益)
   *
   * check below
   * 1. sum(資産の借方)+sum(負債の借方)+sum(資本の借方) = sum(資産の貸方)+sum(負債の貸方)+sum(資本の貸方)
   * 2. sum(資産の残高)+sum(費用の残高)= sum(負債の残高)+sum(資本の残高)+sum(収益の残高)
   */

}
