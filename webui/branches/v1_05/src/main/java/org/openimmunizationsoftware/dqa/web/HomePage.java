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
