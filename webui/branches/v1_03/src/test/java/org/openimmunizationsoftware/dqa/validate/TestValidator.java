package org.openimmunizationsoftware.dqa.validate;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.openimmunizationsoftware.dqa.db.model.PotentialIssue;

import junit.framework.TestCase;

public class TestValidator extends TestCase
{
  public void testValidNameChars()
  {
    assertTrue(Validator.validNameChars("John"));
    assertTrue(Validator.validNameChars("John Henry"));
    assertTrue(Validator.validNameChars("John-Henry"));
    assertTrue(Validator.validNameChars("John's"));
    assertTrue(Validator.validNameChars("John A"));
    assertTrue(Validator.validNameChars("J"));
    assertFalse(Validator.validNameChars("J()"));
  }
  

}
