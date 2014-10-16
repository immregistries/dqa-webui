/*
 * Copyright 2013 by Dandelion Software & Research, Inc (DSR)
 * 
 * This application was written for immunization information system (IIS) community and has
 * been released by DSR under an Apache 2 License with the hope that this software will be used
 * to improve Public Health.  
 */
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
