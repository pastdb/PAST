import org.scalatest.FlatSpec

// Tests that should hold for natural numbers
class MathSpec extends FlatSpec {
  "Addition neutral element" should "be 0" in {
    assert(42 + 0 == 42)
  }

  "Multiplication neutral element" should "be 1" in {
    assert(42 * 1 == 42)
  }
}

