/*
 * Copyright 2013 by Dandelion Software & Research, Inc (DSR)
 * 
 * This application was written for immunization information system (IIS) community and has
 * been released by DSR under an Apache 2 License with the hope that this software will be used
 * to improve Public Health.  
 */
package org.openimmunizationsoftware.dqa.web;

import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.panel.Panel;
import org.hibernate.Session;
import org.openimmunizationsoftware.dqa.manager.DatabaseLogManager;

public class VersionPanel extends Panel
{

  public VersionPanel(String id) {
    super(id);
    DqaSession webSession = (DqaSession) getSession();
    Session dataSession = webSession.getDataSession();

    Label versionLabel = new Label("version", DatabaseLogManager.getVersionDescription(dataSession));
    add(versionLabel);

  }

}
