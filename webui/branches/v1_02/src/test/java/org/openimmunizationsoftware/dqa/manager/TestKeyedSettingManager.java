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
