/*
 * Copyright 2013 by Dandelion Software & Research, Inc (DSR)
 * 
 * This application was written for immunization information system (IIS) community and has
 * been released by DSR under an Apache 2 License with the hope that this software will be used
 * to improve Public Health.  
 */
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
