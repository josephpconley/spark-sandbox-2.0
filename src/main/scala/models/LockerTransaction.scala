package models

import java.sql.Timestamp
import java.util.UUID

import org.joda.time.DateTime
import util.RandomData

case class LockerTransaction(
  TRANSACTION_ID: String,
  TRANSACTION_TIME: Timestamp,
  TRANSACTION_AMOUNT: Double,
  ACCOUNT_ID: String,
  PROGRAMMER_NAME: String,
  PROGRAMMER_ID: String,
  ORIGINATION_POINT: String,
  CLIENT: String,
  RATE_CODE: String,
  CORP: String,
  STATUS: String,
  ERROR_CODE: String,
  ERROR_MESSAGE: String)

object LockerTransaction extends RandomData {

  val statuses = Seq("POSTED","PENDING","FAILED","RETRY","RECONCILED")
  val originationPoints = Seq("VOD","Linear","Unknown")

//  val schema = StructType(Seq(
//    StructField("transaction_id", StringType, nullable = false),
//    StructField("transaction_time", TimestampType, nullable = false),
//    StructField("transaction_amount", DoubleType, nullable = false),
//    StructField("account_id", StringType, nullable = false),
//    StructField("origination_point", StringType, nullable = true),
//    StructField("client", StringType, nullable = false),
//    StructField("corp", StringType, nullable = true),
//    StructField("programmer_id", StringType, nullable = true),
//    StructField("programmer_name", StringType, nullable = true),
//    StructField("rate_code", StringType, nullable = true),
//    StructField("status", StringType, nullable = false),
//    StructField("error_code", StringType, nullable = true),
//    StructField("error_message", StringType, nullable = true)
//  ))


  def generate(n: Int, numMonths: Int = 12): Seq[LockerTransaction] = {
    val maxDate = DateTime.now()
    val minDate = maxDate.minusMonths(numMonths)

    (0 until n).map { _ =>
      LockerTransaction(
        TRANSACTION_ID = UUID.randomUUID().toString,
        TRANSACTION_TIME = new Timestamp(generateDate(minDate, maxDate).getMillis),
        TRANSACTION_AMOUNT = generateDouble(0, 25),
        ACCOUNT_ID = generateNumeric(16),
        PROGRAMMER_NAME = generateAlpha(25, 50),
        PROGRAMMER_ID = generateAlphaNumeric(12),
        ORIGINATION_POINT = generateFromSeq(originationPoints),
        CLIENT = "Native",
        RATE_CODE = generateAlphaNumeric(2, 5),
        CORP = generateNumeric(6),
        STATUS = generateFromSeq(statuses),
        ERROR_CODE = "0000",
        ERROR_MESSAGE = "no error"
      )
    }
  }
}