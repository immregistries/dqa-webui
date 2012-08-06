package org.openimmunizationsoftware.dqa.db.model;

import java.util.List;


import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.openimmunizationsoftware.dqa.db.model.VaccineCpt;
import org.openimmunizationsoftware.dqa.db.model.VaccineProduct;
import org.openimmunizationsoftware.dqa.manager.OrganizationManager;

import junit.framework.TestCase;

public class TestVaccineProduct extends TestCase
{
  public void testVaccineProduct()
  {
    SessionFactory factory = OrganizationManager.getSessionFactory();
    Session session = factory.openSession();

    Query query = session.createQuery("from VaccineProduct");
    List<VaccineProduct> vaccineProducts = query.list();
    session.close();
    assertEquals(81, vaccineProducts.size());
  }
  
  public void testVaccineCpt()
  {
    SessionFactory factory = OrganizationManager.getSessionFactory();
    Session session = factory.openSession();

    Query query = session.createQuery("from VaccineCpt");
    List<VaccineCpt> vaccineCpts = query.list();
    session.clear();
    assertEquals(98, vaccineCpts.size());
    
  }
}
