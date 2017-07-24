package app.sql

import java.util.Properties

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import util.BaseDriver

object Oracle extends BaseDriver {

  override def run = {

    val dateTimeFmt = "yyyy-MM-dd HH:mm:ss"
    val dateTimeFormatter = DateTimeFormat.forPattern(dateTimeFmt)
    val DB_DATE_FMT = "yyyy-mm-dd HH24:MI:SS"

    val startDate = DateTime.parse("2017-01-30 00:00:00", dateTimeFormatter)
    val endDate = DateTime.parse("2017-01-31 00:00:00", dateTimeFormatter)

    val sql = legacySQL(startDate.toString(dateTimeFmt), endDate.toString(dateTimeFmt))

    val duration: Int = 24 * 60 * 60
    val numPartitions = 10
    val step: Int = duration / numPartitions

    val partitionDateRanges = (0 until duration by step).map { offset =>
      val start = startDate.plusSeconds(offset).toString(dateTimeFmt)
      val end = startDate.plusSeconds(offset + step).toString(dateTimeFmt)

      s"""
         |(ENT_REQUESTED_TIMESTAMP >= to_date('$start', '$DB_DATE_FMT') AND
         |ENT_REQUESTED_TIMESTAMP < to_date('$end', '$DB_DATE_FMT'))
       """.stripMargin
    }.toArray

    val df = sqlContext.read.jdbc(
      url = "jdbc:oracle:thin:ues/ues@//esdb-wc-grdb.sys.comcast.net:1521/esdbwc",
      table = s"($sql)",
      predicates = partitionDateRanges,
      connectionProperties = new Properties)

    logger.info(df.count())
  }

  def legacySQL(startDateStr: String, endDateStr: String) =
    s"""
      |---WC Base Query
      |SELECT DISTINCT
      |UD.ENT_REQUESTED_TIMESTAMP,
      |to_timestamp(UD.ENT_REQUESTED_TIMESTAMP) as transaction_time,
      |UD.TRACKING_ID as transaction_id,
      |(ORC.PRICE / 100) as transaction_amount,
      |UD.ACCOUNT_NUM as account_id,
      |CASE WHEN S.SVC_DESCRIPTION is null then 'Unknown' ELSE S.SVC_DESCRIPTION END as programmer_name,
      |UD.SERVICE_ID as programmer_id,
      |CASE SUBSTR(UD.TRACKING_ID,1,3)
      |    WHEN 'UPS'  THEN 'VOD'
      |    WHEN 'LUS'  THEN 'Linear'
      |    WHEN 'ODL'  THEN 'Online'
      |    ELSE 'Unknown'
      |  END AS origination_point,
      |'Native' as client,
      |UD.RATE_CODE AS rate_code,
      |UD.CORP as corp,
      |UD.RECONCILE_STATUS as status,
      |--'WC' AS DC,
      |CASE WHEN UEH.ERROR_CODE is null then '0000' ELSE UEH.ERROR_CODE END as error_code,
      |CASE WHEN UEH.ERROR_MESSAGE is null then '0000' ELSE UEH.ERROR_MESSAGE END as error_message
      |FROM UPSELL_ITEM_DETAILS UD
      |join UES.UPSELL_OFFER_RC ORC on UD.RATE_CODE=ORC.RATECODE AND UD.CORP=ORC.CORP
      |JOIN UES.UPSELL_OFFERS UO on ORC.OFFER_ID=UO.OFFER_ID
      | join UES.SERVICE_IDS S on UD.SERVICE_ID=S.SERVICE_ID
      |left outer JOIN UPSELL.UPSELL_ERROR_HISTORY UEH on UD.TRACKING_ID=UEH.TRACKING_ID AND ERROR_MESSAGE not in ('A system error occured when placing your order. Please contact a system administrator.','SERV_CD','System Exception','DS_CD','CSV04E--DUPLICATE SERVICE CODE FOUND','EQP_TYPE')
      |WHERE ENT_REQUESTED_TIMESTAMP BETWEEN UO.OFFER_START_DTTM and UO.OFFER_END_DTTM
      |
      |UNION
      |
      |---CMC Query
      |SELECT DISTINCT
      |UD.ENT_REQUESTED_TIMESTAMP,
      |to_timestamp(UD.ENT_REQUESTED_TIMESTAMP) as transaction_time,
      |UD.TRACKING_ID as transaction_id,
      |(ORC.PRICE / 100) as transaction_amount,
      |UD.ACCOUNT_NUM as account_id,
      |CASE WHEN S.SVC_DESCRIPTION is null then 'Unknown' ELSE S.SVC_DESCRIPTION END as programmer_name,
      |UD.SERVICE_ID as programmer_id,
      |CASE SUBSTR(UD.TRACKING_ID,1,3)
      |    WHEN 'UPS'  THEN 'VOD'
      |    WHEN 'LUS'  THEN 'Linear'
      |    WHEN 'ODL'  THEN 'Online'
      |    ELSE 'Unknown'
      |  END AS origination_point,
      |'Native' as client,
      |UD.RATE_CODE AS rate_code,
      |UD.CORP as corp,
      |UD.RECONCILE_STATUS as status,
      |--'CMC' AS DC,
      |CASE WHEN UEH.ERROR_CODE is null then '0000' ELSE UEH.ERROR_CODE END as error_code,
      |CASE WHEN UEH.ERROR_MESSAGE is null then '0000' ELSE UEH.ERROR_MESSAGE END as error_message
      |FROM UPSELL_ITEM_DETAILS@UPSELL_CMC UD
      |join UES.UPSELL_OFFER_RC ORC on UD.RATE_CODE=ORC.RATECODE AND UD.CORP=ORC.CORP
      |JOIN UES.UPSELL_OFFERS UO on ORC.OFFER_ID=UO.OFFER_ID
      |join UES.SERVICE_IDS S on UD.SERVICE_ID=S.SERVICE_ID
      |left outer JOIN UPSELL.UPSELL_ERROR_HISTORY@UPSELL_CMC UEH on UD.TRACKING_ID=UEH.TRACKING_ID AND ERROR_MESSAGE not in ('A system error occured when placing your order. Please contact a system administrator.','SERV_CD','System Exception','DS_CD','EQP_TYPE')
      |WHERE ENT_REQUESTED_TIMESTAMP BETWEEN UO.OFFER_START_DTTM and UO.OFFER_END_DTTM
      |
      |UNION
      |
      |---CH2 Base Query
      |SELECT DISTINCT
      |UD.ENT_REQUESTED_TIMESTAMP,
      |to_timestamp(UD.ENT_REQUESTED_TIMESTAMP) as transaction_time,
      |UD.TRACKING_ID as transaction_id,
      |(ORC.PRICE / 100) as transaction_amount,
      |UD.ACCOUNT_NUM as account_id,
      |CASE WHEN S.SVC_DESCRIPTION is null then 'Unknown' ELSE S.SVC_DESCRIPTION END as programmer_name,
      |UD.SERVICE_ID as programmer_id,
      |CASE SUBSTR(UD.TRACKING_ID,1,3)
      |    WHEN 'UPS'  THEN 'VOD'
      |    WHEN 'LUS'  THEN 'Linear'
      |    WHEN 'ODL'  THEN 'Online'
      |    ELSE 'Unknown'
      |  END AS origination_point,
      |'Native' as client,
      |UD.RATE_CODE AS rate_code,
      |UD.CORP as corp,
      |UD.RECONCILE_STATUS as status,
      |--'CH2' AS DC,
      |CASE WHEN UEH.ERROR_CODE is null then '0000' ELSE UEH.ERROR_CODE END as error_code,
      |CASE WHEN UEH.ERROR_MESSAGE is null then '0000' ELSE UEH.ERROR_MESSAGE END as error_message
      |FROM UPSELL_ITEM_DETAILS@UPSELL_CH2 UD
      |join UES.UPSELL_OFFER_RC ORC on UD.RATE_CODE=ORC.RATECODE AND UD.CORP=ORC.CORP
      |JOIN UES.UPSELL_OFFERS UO on ORC.OFFER_ID=UO.OFFER_ID
      | join UES.SERVICE_IDS S on UD.SERVICE_ID=S.SERVICE_ID
      |left outer JOIN UPSELL.UPSELL_ERROR_HISTORY@UPSELL_CH2 UEH on UD.TRACKING_ID=UEH.TRACKING_ID AND ERROR_MESSAGE not in ('A system error occured when placing your order. Please contact a system administrator.','SERV_CD','System Exception','DS_CD','CSV04E--DUPLICATE SERVICE CODE FOUND','EQP_TYPE')
      |WHERE ENT_REQUESTED_TIMESTAMP BETWEEN UO.OFFER_START_DTTM and UO.OFFER_END_DTTM
    """.stripMargin.trim
}