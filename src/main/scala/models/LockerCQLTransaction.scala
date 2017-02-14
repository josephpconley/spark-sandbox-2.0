package models

import java.sql.Timestamp
import java.util.UUID

import org.joda.time.DateTime
import util.RandomData

case class LockerCQLTransaction(
  transaction_id: String,
  transaction_time: Timestamp,
  transaction_amount: Double,
  account_id: String,
  programmer_name: String,
  programmer_id: String,
  origination_point: String,
  client: String,
  rate_code: String,
  corp: String,
  status: String,
  error_code: String,
  error_message: String)

object LockerCQLTransaction extends RandomData {

  val statuses = Seq("POSTED","PENDING","FAILED","RETRY","RECONCILED")
  val originationPoints = Seq("VOD","Linear","Unknown")

  def generate(n: Int): Seq[LockerCQLTransaction] = {
    val maxDate = DateTime.now()
    val minDate = maxDate.minusMonths(6)

    (0 to n).map { _ =>
      LockerCQLTransaction(
        transaction_id = UUID.randomUUID().toString,
        transaction_time = new Timestamp(generateDate(minDate, maxDate).getMillis),
        transaction_amount = generateDouble(0, 25),
        account_id = generateNumeric(16),
        programmer_name = generateAlpha(25, 50),
        programmer_id = generateAlphaNumeric(12),
        origination_point = generateFromSeq(originationPoints),
        client = "Native",
        rate_code = generateAlphaNumeric(2, 5),
        corp = generateNumeric(6),
        status = generateFromSeq(statuses),
        error_code = "0000",
        error_message = "no error"
      )
    }
  }
}