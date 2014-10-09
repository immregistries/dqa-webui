package org.openimmunizationsoftware.dqa.manager;

import java.util.List;

import org.openimmunizationsoftware.dqa.db.model.VaccineGroup;

import junit.framework.TestCase;

public class TestVaccineGroupManager extends TestCase
{
  public void testGetVaccineGroupManager()
  {
    VaccineGroupManager vaccineGroupManager = VaccineGroupManager.getVaccineGroupManager();
    List<VaccineGroup> vaccineGroupList = vaccineGroupManager.getVaccineGroupList(VaccineGroup.GROUP_STATUS_EXPECTED);
    assertEquals(10, vaccineGroupList.size());
    VaccineGroup hepA = null;
    for (VaccineGroup vaccineGroup: vaccineGroupList)
    {
      vaccineGroup.getGroupLabel();
      if (vaccineGroup.getGroupCode().equals("HepA"))
      {
        hepA = vaccineGroup;
      }
    }
    assertNotNull(hepA);
    assertEquals(6, hepA.getVaccineCvxList().size());
  }
}
