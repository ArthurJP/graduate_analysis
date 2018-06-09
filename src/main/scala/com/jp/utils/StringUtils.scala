package com.jp.utils

class StringUtils(val str: String) {

  def toIntPlus: Int = {
    try {
      str.replaceAll("\"","").toInt
    } catch {
      case _: Exception => 0
    }
  }

  def toStringPlus: String = {
    try {
      str.replaceAll("\"","").toString
    } catch {
      case _: Exception => ""
    }
  }

  def toLongPlus: Long = {
    try {
      str.toLong
    } catch {
      case _: Exception => 0L
    }
  }

  def toDoublePlus: Double = {
    try {
      str.toDouble
    } catch {
      case _: Exception => 0.0
    }
  }

}

object StringUtils {
  implicit def StringUtils(str: String) = new StringUtils(str)
}
