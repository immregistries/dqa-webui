/*
 * Copyright 2013 by Dandelion Software & Research, Inc (DSR)
 * 
 * This application was written for immunization information system (IIS) community and has
 * been released by DSR under an Apache 2 License with the hope that this software will be used
 * to improve Public Health.  
 */
package org.openimmunizationsoftware.dqa.manager;

import junit.framework.TestCase;

public class TestHashManager extends TestCase
{
  private static final String TEST_1 = "Test";
  private static final String TEST_2 = "Test2";
  private static final String TEST_3 = "Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String "
      + "Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String "
      + "Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String "
      + "Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String "
      + "Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String "
      + "Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String "
      + "Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String "
      + "Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String "
      + "Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String "
      + "Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String "
      + "Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String "
      + "Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String "
      + "Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String "
      + "Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String "
      + "Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String "
      + "Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String "
      + "Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String "
      + "Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String "
      + "Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String "
      + "Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String "
      + "Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String "
      + "Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String "
      + "Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String "
      + "Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String "
      + "Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String "
      + "Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String "
      + "Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String "
      + "Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String "
      + "Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String "
      + "Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String "
      + "Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String "
      + "Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String "
      + "Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String "
      + "Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String "
      + "Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String "
      + "Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String "
      + "Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String "
      + "Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String "
      + "Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String "
      + "Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String "
      + "Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String "
      + "Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String "
      + "Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String "
      + "Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String "
      + "Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String "
      + "Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String "
      + "Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String "
      + "Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String "
      + "Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String "
      + "Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String "
      + "Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String "
      + "Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String "
      + "Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String "
      + "Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String "
      + "Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String "
      + "Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String "
      + "Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String "
      + "Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String Very Long String ";

  public void testHash() throws Exception
  {
    String result = HashManager.getMD5Hash(TEST_1);
    assertNotNull(result);
    assertEquals("0CBC6611F5540BD0809A388DC95A615B", result);
    result = HashManager.getMD5Hash(TEST_1);
    assertNotNull(result);
    assertEquals("0CBC6611F5540BD0809A388DC95A615B", result);
    result = HashManager.getMD5Hash(TEST_2);
    assertNotNull(result);
    assertEquals("C454552D52D55D3EF56408742887362B", result);
    result = HashManager.getMD5Hash(TEST_3);
    assertNotNull(result);
    assertEquals("1BB206401C39DE2BA21398DAFE3219AA", result);
  }
}
