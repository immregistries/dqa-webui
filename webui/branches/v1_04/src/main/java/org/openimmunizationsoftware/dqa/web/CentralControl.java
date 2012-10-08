package org.openimmunizationsoftware.dqa.web;

import org.hibernate.SessionFactory;
import org.hibernate.cfg.AnnotationConfiguration;

public class CentralControl {
	  
	  private static SessionFactory factory;
	  
	  public static SessionFactory getSessionFactory()
	  {
	    if (factory == null)
	    {
	      factory = new AnnotationConfiguration().configure().buildSessionFactory();
	    }
	    return factory;
	  }
}
