module bookkeeping.asset;

/*************************************************************
 * 有形資産の減価償却
 *************************************************************/
struct FixedAsset{
  import std.datetime: Date, Month;

  string assetName;

  this(in string name, in Date acqDate, in string titleStr,
       in uint price, in uint period) @safe pure nothrow @nogc{
    assetName= name;
    _acquisitionDate= acqDate;
    _accountTitle= titleStr;
    _priceInit= price;
    _economicLife= period;
  }

  @property @safe pure nothrow @nogc const{
    /*****************************************
     * 会計科目
     *****************************************/
    string accountTitle(){
      return _accountTitle;
    }

    /*****************************************
     * 年間の償却金額
     *
     * 小数点以下は切り上げ
     * See_Also:
     * 国税庁[https://www.keisan.nta.go.jp/h29yokuaru/aoiroshinkoku/hitsuyokeihi/genkashokyakuhi/hasushori.html]
     *****************************************/
    T priceDeprecBasic(T= uint)(){
      import std.traits: isFloatingPoint;
      const double result= cast(double)(_priceInit)/_economicLife;
      static if(isFloatingPoint!T){
	return cast(T)result;
      }
      else{
	import std.math: ceil;
	return cast(T)(ceil(result));
      }
    }

    /*****************************************
     * 償却期間
     *****************************************/
    uint economicLife(){
      return _economicLife;
    }
  }

  @safe pure const{
    /*****************************************
     * 計算上の取得年月
     *****************************************/
    Date acquisitionDateNominal(){
      return Date(_acquisitionDate.year, _acquisitionDate.month, 1);
    }

    /*****************************************
     * 計算上の償却期間終了年月
     *****************************************/
    Date endDateNominal(){
      import std.datetime: dur;
      Date st= acquisitionDateNominal;
      return Date(st.year+economicLife, st.month, 1) -dur!"days"(1);
    }

    /*****************************************
     * 最初の年の有効残月数
     *****************************************/
    ubyte remainMonthOfFirstYear() nothrow @nogc{
      uint result= 13-_acquisitionDate.month;
      return cast(typeof(return))result;
    }

    /*****************************************
     * 償却期間内か否か
     *****************************************/
    bool isInDeprecPeriod(in uint year, in Month month= Month.dec){
      const Date theDay= Date(year, month, daysOfMonth(year, month));
      return (theDay >= acquisitionDateNominal &&
	      theDay <= endDateNominal)? true: false;
    }

    bool isInDeprecPeriod(in Date theDay){
      return isInDeprecPeriod(theDay.year, theDay.month);
    }

    /*****************************************
     * 資産額
     *****************************************/
    uint assetValue(in uint year, in Month month= Month.dec){
      import std.math: ceil;
      const Date theDay= Date(year, month, daysOfMonth(year, month));
      double result;

      if(theDay < acquisitionDateNominal){	// 取得前
	result= 0;
      }
      else{	// 取得後
	/*
	 * acquisition: 2010-02
	 * calc: 2012-11
	 * duration= -1+12*2+11= 34
	 *
	 * 2012-2010+ 11-2+1
	 */
	if(isInDeprecPeriod(year, month)){
	  const uint durInMonth= theDay.year-acquisitionDateNominal.year
	    +theDay.month-acquisitionDateNominal.month+1;
	  double price= _priceInit -ceil(priceDeprecBasic!double/12*durInMonth);
	}
	else{
	  result= 1;	// 備忘価格
	}
      }
      return cast(typeof(return))result;
    }

    /*****************************************
     * 償却金額
     *****************************************/
    uint priceDeprec(string Period)(in uint year, in Month month= Month.dec)
    if(Period == "year" || Period == "month"){
      typeof(return) result;

      if(isInDeprecPeriod(year, month)){
	static if(Period == "year"){
	  if(year == _acquisitionDate.year) result= priceDeprecFirst;
	  else result= assetValue(year-1)-assetValue(year);
	}
	else{
	  import std.math: ceil;
	  return cast(typeof(return))ceil(priceDeprecBasic!double/12);
	}
      }
      else{
	result= 0;
      }
      return result;
    }
  }

private:
  Date _acquisitionDate;
  uint _priceInit;
  uint _economicLife;
  string _accountTitle;

  @safe pure nothrow @nogc{
    uint priceDeprecFirst() const{
      import std.math: ceil;
      const double validRate= cast(double)(remainMonthOfFirstYear)/12;
      return cast(typeof(return))ceil(validRate*this.priceDeprecBasic!double);
    }

    uint daysOfMonth(in uint year, in Month month) const{
      typeof(return) result;
      final switch(month){
      case Month.jan, Month.mar, Month.may, Month.jul, Month.aug, Month.oct, Month.dec:
	result= 31;
	break;
      case Month.apr, Month.jun, Month.sep, Month.nov:
	result= 30;
	break;
      case Month.feb:
	result= (year%400 == 0 || (year%4 == 0 && year%100 != 0))? 29: 28;
      }
      return result;
    }
  }
}
