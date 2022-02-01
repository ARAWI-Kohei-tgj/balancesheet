/**********************************************
 * Transaction table `shipment_quantity'
 *
 * 1| shipment_date: 出荷日
 * 2| crop_name: 作物名
 * 3| class_: 出荷規格 shipment_class.class_list内に存在すること
 * 4| nominal_mass: 公称総重量[kg]
 **********************************************/


/**********************************************
 * Transaction table `shipment_costs'
 *
 * shipment_date: 出荷日
 * crop_name: 作物名
 * market_fee: 市場手数料
 * ja_fee: 農協手数料
 * fare: 運賃
 * insurance: 保険負担金
 **********************************************/


/**********************************************
 * Transaction table `sale_price'
 *
 * 単価の履歴
 *
 * shipment_date: 出荷日
 * crop_name: 作物名
 * class_: 出荷規格
 * unit_price: 単価 [JPY/unit_mass]
 * unit_mass: 単位重量[g]
 **********************************************/


/**********************************************
 * Transaction table `shipment_insentive'
 *
 * 出荷奨励金の履歴
 *
 * shipment_date: 出荷日
 * crop_name: 作物名
 * price: 出荷奨励金[JPY]
 **********************************************/


/**********************************************
 * Transaction table `shipment_reward'
 *
 * 1| shipment_date: 出荷日
 * 2| crop_name: 作物名
 * 3| payment_date: 振込日
 * 4| pay_from: 振込元
 **********************************************/


/**********************************************
 * Transaction table `shipment_package'
 *
 * 1| shipment_date: 出荷日
 * 2| crop_name: 作物名
 * 3| package_mass: 荷姿の質量
 * 4| quantity: 個数
 **********************************************/

select sum()
from shipment_quantity
where shipment_date = to_date('', 'YYYY-MM-DD');
