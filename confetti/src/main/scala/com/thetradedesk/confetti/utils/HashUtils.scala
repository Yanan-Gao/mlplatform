package com.thetradedesk.confetti.utils

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.Base64

object HashUtils {
  def sha256Base64(content: String): String = {
    val digest = MessageDigest.getInstance("SHA-256").digest(content.getBytes(StandardCharsets.UTF_8))
    Base64.getUrlEncoder.encodeToString(digest)
  }
}

