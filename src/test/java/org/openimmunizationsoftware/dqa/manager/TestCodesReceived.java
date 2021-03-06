/*
 * Copyright 2013 by Dandelion Software & Research, Inc (DSR)
 * 
 * This application was written for immunization information system (IIS) community and has
 * been released by DSR under an Apache 2 License with the hope that this software will be used
 * to improve Public Health.  
 */
package org.openimmunizationsoftware.dqa.manager;

import junit.framework.TestCase;

import org.openimmunizationsoftware.dqa.db.model.CodeReceived;
import org.openimmunizationsoftware.dqa.db.model.CodeTable;

public class TestCodesReceived extends TestCase
{

  public void testGetCodesReceived()
  {
    CodesReceived codesReceived = CodesReceived.getCodesReceived();
    assertEquals(43, codesReceived.getCodeTables().size());
    CodeReceived cr = codesReceived.getCodeReceived("90700", CodesReceived.getCodeTable(CodeTable.Type.VACCINATION_CPT_CODE));
    assertNotNull(cr);
    cr = codesReceived.getCodeReceived("03", CodesReceived.getCodeTable(CodeTable.Type.VACCINATION_CVX_CODE));
    assertNotNull(cr);
    cr = codesReceived.getCodeReceived("00", CodesReceived.getCodeTable(CodeTable.Type.VACCINATION_INFORMATION_SOURCE));
    assertNotNull(cr);
    cr = codesReceived.getCodeReceived("IM", CodesReceived.getCodeTable(CodeTable.Type.BODY_ROUTE));
    assertNotNull(cr);
    assertEquals("V", cr.getCodeStatus().getCodeStatus());
    cr = codesReceived.getCodeReceived("MTH", CodesReceived.getCodeTable(CodeTable.Type.PERSON_RELATIONSHIP));
    assertNotNull(cr);
  }

}
