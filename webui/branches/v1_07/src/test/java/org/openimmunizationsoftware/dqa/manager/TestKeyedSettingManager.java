/*
 * Copyright 2013 by Dandelion Software & Research, Inc (DSR)
 * 
 * This application was written for immunization information system (IIS) community and has
 * been released by DSR under an Apache 2 License with the hope that this software will be used
 * to improve Public Health.  
 */
package org.openimmunizationsoftware.dqa.manager;

import org.openimmunizationsoftware.dqa.db.model.Application;

import junit.framework.TestCase;

public class TestKeyedSettingManager extends TestCase
{

   public void testGetKeyedSettingManager()
   {
     KeyedSettingManager ksm = KeyedSettingManager.getKeyedSettingManager();
     assertNotNull(ksm);
     assertNotNull(KeyedSettingManager.getApplication());
     Application application = KeyedSettingManager.getApplication();
     assertTrue(application.getApplicationLabel().length() > 0);
     assertTrue(application.getRunThis());
   }
}
