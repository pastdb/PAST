import org.scalatest.FlatSpec

class RegexUtilsSpec extends FlatSpec {
  "RegexOps.accepts" should "only fully match" in {
    import past.util.RegexUtil._
    assert("abc".r accepts "abc")
    assertResult("a".r accepts "abc")(false)
  }
}

