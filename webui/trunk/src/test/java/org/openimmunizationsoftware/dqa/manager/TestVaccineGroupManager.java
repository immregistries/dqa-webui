/*
 * Copyright 2013 by Dandelion Software & Research, Inc (DSR)
 * 
 * This application was written for immunization information system (IIS) community and has
 * been released by DSR under an Apache 2 License with the hope that this software will be used
 * to improve Public Health.  
 */
package org.openimmunizationsoftware.dqa.manager;

import java.util.List;

import org.openimmunizationsoftware.dqa.db.model.VaccineGroup;

import junit.framework.TestCase;

public class TestVaccineGroupManager extends TestCase
{
  public void testGetVaccineGroupManager()
  {
    VaccineGroupManager vaccineGroupManager = VaccineGroupManager.getVaccineGroupManager();
    // TODO 
  }
}
