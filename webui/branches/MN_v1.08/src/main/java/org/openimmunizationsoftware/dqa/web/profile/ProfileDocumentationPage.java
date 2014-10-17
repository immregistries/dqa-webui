/*
 * Copyright 2013 by Dandelion Software & Research, Inc (DSR)
 * 
 * This application was written for immunization information system (IIS) community and has
 * been released by DSR under an Apache 2 License with the hope that this software will be used
 * to improve Public Health.  
 */
package org.openimmunizationsoftware.dqa.web.profile;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.wicket.markup.html.WebMarkupContainer;
import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.link.BookmarkablePageLink;
import org.apache.wicket.markup.html.list.ListItem;
import org.apache.wicket.markup.html.list.ListView;
import org.apache.wicket.request.mapper.parameter.PageParameters;
import org.hibernate.Query;
import org.hibernate.Session;
import org.openimmunizationsoftware.dqa.db.model.PotentialIssue;
import org.openimmunizationsoftware.dqa.db.model.PotentialIssueStatus;
import org.openimmunizationsoftware.dqa.db.model.SubmitterProfile;
import org.openimmunizationsoftware.dqa.manager.PotentialIssues;
import org.openimmunizationsoftware.dqa.manager.PotentialIssues.Field;
import org.openimmunizationsoftware.dqa.web.DqaBasePage;
import org.openimmunizationsoftware.dqa.web.DqaSession;
import org.openimmunizationsoftware.dqa.web.NavigationPanel;
import org.openimmunizationsoftware.dqa.web.SecurePage;

public class ProfileDocumentationPage extends DqaBasePage implements SecurePage
{

  private static final long serialVersionUID = 1L;
  
  private boolean errorsOnly = false;

  public ProfileDocumentationPage() {
    this(new PageParameters());
  }

  public ProfileDocumentationPage(final PageParameters parameters) {
    super(parameters, NavigationPanel.PROFILE);

    errorsOnly = parameters.get("errorsOnly").toBoolean(false);
    

    
    final DqaSession webSession = (DqaSession) getSession();
    final Session dataSession = webSession.getDataSession();
    final SubmitterProfile profile = webSession.getSubmitterProfile();
    final PotentialIssues potentialIssues = PotentialIssues.getPotentialIssues();
    
    WebMarkupContainer allExplanation = new WebMarkupContainer("allExplanation");
    add(allExplanation);
    allExplanation.setVisible(!errorsOnly);
    PageParameters pars = new PageParameters();
    pars.add("errorsOnly", true);
    allExplanation.add(new BookmarkablePageLink<Void>("showOnlyErrorsLink", ProfileDocumentationPage.class, pars));
    
    WebMarkupContainer errorsOnlyExplanation = new WebMarkupContainer("errorsOnlyExplanation");
    add(errorsOnlyExplanation);
    errorsOnlyExplanation.setVisible(errorsOnly);
    errorsOnlyExplanation.add(new BookmarkablePageLink<Void>("showAllLink", ProfileDocumentationPage.class));

    List<Field> fieldList = potentialIssues.getAllFields();
    Collections.sort(fieldList);

    Query query = dataSession.createQuery("from PotentialIssueStatus where profile = ?");
    query.setParameter(0, profile);
    List<PotentialIssueStatus> potentialIssueStatusList = query.list();
    final Map<PotentialIssue, PotentialIssueStatus> potentialIssueStatusMap = new HashMap<PotentialIssue, PotentialIssueStatus>();
    for (PotentialIssueStatus potentialIssueStatus : potentialIssueStatusList)
    {
      potentialIssueStatusMap.put(potentialIssueStatus.getIssue(), potentialIssueStatus);
    }

    ListView<Field> documentationItems = new ListView<Field>("documentationItems", fieldList) {
      @Override
      protected void populateItem(ListItem<Field> item)
      {
        Field field = item.getModelObject();
        item.add(new Label("documentationItemText", potentialIssues.getDocumentation(field, potentialIssueStatusMap, errorsOnly))
            .setEscapeModelStrings(false));
      }
    };
    add(documentationItems);
  }
}