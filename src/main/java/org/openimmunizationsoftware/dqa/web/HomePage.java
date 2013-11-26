/*
 * Copyright 2013 by Dandelion Software & Research, Inc (DSR)
 * 
 * This application was written for immunization information system (IIS) community and has
 * been released by DSR under an Apache 2 License with the hope that this software will be used
 * to improve Public Health.  
 */
package org.openimmunizationsoftware.dqa.web;


import org.apache.wicket.request.mapper.parameter.PageParameters;

/**
 * Homepage
 */
public class HomePage extends DqaBasePage 
{

  private static final long serialVersionUID = 1L;
 

  // TODO Add any page properties or variables here

  /**
   * Constructor that is invoked when page is invoked without a session.
   * 
   * @param parameters
   *          Page parameters
   */
  public HomePage(final PageParameters parameters) {
    super(parameters, NavigationPanel.HOME_LOGGED_IN);
  }
  
}
