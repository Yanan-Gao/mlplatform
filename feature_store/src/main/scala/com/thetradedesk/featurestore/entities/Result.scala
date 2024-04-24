package com.thetradedesk.featurestore.entities

case class Result(message: String, success: Boolean)

object Result {
  private val success: Result = Result("OK", success = true)

  def failed(message: String): Result = {
    Result(message, success = false)
  }

  def succeed(): Result = {
    success
  }

  def succeed(message: String): Result = {
    Result(message, success = true)
  }
}
