package org.openimmunizationsoftware.dqa.validate.immtrac;

import junit.framework.TestCase;

public class TestPfsSupport extends TestCase
{
  private static String[] TEST_PFS = { "1124800189", "1124800189", "1124800189", "1124800189", "1124800189",
      "1124800189", "1125880009", "1130020000", "1138440014", "1138440023", "1138440032", "1138440032", "1138440050",
      "1138440069", "1138440069", "1138440069", "1138440078", "1138440087", "1138440096", "1138440096", "1138440103",
      "1138440112", "1138440121", "1138440130", "1138440149", "1138440158", "1138440158", "1138440167", "1138440176",
      "1138440185", "1138440194", "1138440201", "1138440210", "1138440229", "1138440238", "1138440247", "1138440256",
      "1138440265", "1138440274", "1138440283", "1138440292", "1138440309", "1138440309", "1138440318", "1138440354",
      "1138440363", "1138440372", "1138440381", "1140670002", "1147140009", "1147150006", "1147230000", "1147300005",
      "1148080009", "1148080009", "1148400002", "1148400002", "1148410000", "1148420008", "1148550000", "1152540005",
      "1152540005", "1152560000", "1152560000", "1160270004" };

  public void testVerifyCheckDigit()
  {
    assertFalse(PfsSupport.verifyCorrect(""));
    assertFalse(PfsSupport.verifyCorrect("Apple"));
    assertFalse(PfsSupport.verifyCorrect("1234567890"));
    assertFalse(PfsSupport.verifyCorrect("1111111111"));
    assertFalse(PfsSupport.verifyCorrect("123"));
    assertFalse(PfsSupport.verifyCorrect("2222222222"));
    assertFalse(PfsSupport.verifyCorrect("22222222222"));
    for (String pfs : TEST_PFS) {
      assertTrue(pfs, PfsSupport.verifyCorrect(pfs));
    }
  }
}
