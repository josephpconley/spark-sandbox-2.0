package util

import java.util.concurrent.ThreadLocalRandom

import org.joda.time.DateTime

import scala.util.Random

trait RandomData {

  val digits = 0 to 9

  def generateDate(min: DateTime, max: DateTime) = {
    val random = new Random()
    val diff: Long = max.getMillis - min.getMillis
    new DateTime(min.getMillis + (diff * random.nextDouble()).toLong)
  }

  def generateDouble(min: Double, max: Double) = ThreadLocalRandom.current().nextDouble(min, max)

  def generateNumeric(length: Int) = {
    val random = new Random()
    (0 until length).map(_ => digits(random.nextInt(9))).mkString
  }

  def generateAlpha(minLength: Int, maxLength: Int): String = {
    val random = new Random()
    val diff = maxLength - minLength
    val length = minLength + (diff * random.nextDouble()).toInt

    random.alphanumeric.filter(isAlpha).take(length).mkString
  }

  def generateAlphaNumeric(length: Int) = {
    val random = new Random()
    random.alphanumeric.take(length - 1).mkString
  }

  def generateAlphaNumeric(minLength: Int, maxLength: Int): String = {
    val random = new Random()
    val diff = maxLength - minLength
    val length = minLength + (diff * random.nextDouble()).toInt

    random.alphanumeric.take(length).mkString
  }

  def generateFromSeq(values: Seq[String]): String = {
    val random = new Random()
    values(random.nextInt(values.size))
  }

  private def isAlpha(c: Char) = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')
}

object RandomData extends RandomData