/*
 * Copyright 2013 by Dandelion Software & Research, Inc (DSR)
 * 
 * This application was written for immunization information system (IIS) community and has
 * been released by DSR under an Apache 2 License with the hope that this software will be used
 * to improve Public Health.  
 */
package org.openimmunizationsoftware.dqa.db.model.received;

import org.openimmunizationsoftware.dqa.db.model.received.types.PhoneNumber;

import junit.framework.TestCase;

public class TestPhoneNumber extends TestCase
{
  public void testPhoneNumber()
  {
    PhoneNumber phoneNumber = new PhoneNumber();
    phoneNumber.setNumber("(555)555-4841");
    assertEquals("(555)555-4841", phoneNumber.getNumber());
    assertEquals("5554841", phoneNumber.getLocalNumber());
    assertEquals("555", phoneNumber.getAreaCode());
  }
}
