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
