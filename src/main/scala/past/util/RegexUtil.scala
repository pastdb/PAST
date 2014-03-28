package past.util

import scala.util.matching.Regex

/**
 * Utilities for Regex objects.
 */
object RegexUtil {
  /** Missing methods for Regex objects. */
  implicit class RegexOps(regex: Regex) {
    /**
     * Returns `true` if the `regex` __fully__ matches `str`.
     * Note the "fully" part: the regex has to match `str` from beginning
     * to end. That is, `"b".r accepts "ab"` returns `false`.
     */
    def accepts(str: String): Boolean = regex.pattern.matcher(str).matches
  }
}

