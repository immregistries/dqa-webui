package org.openimmunizationsoftware.dqa.web.batch;

import java.text.SimpleDateFormat;
import java.util.List;

import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.link.Link;
import org.apache.wicket.markup.html.list.ListItem;
import org.apache.wicket.markup.html.list.ListView;
import org.apache.wicket.request.mapper.parameter.PageParameters;
import org.hibernate.Query;
import org.hibernate.Session;
import org.openimmunizationsoftware.dqa.db.model.BatchActions;
import org.openimmunizationsoftware.dqa.db.model.BatchReport;
import org.openimmunizationsoftware.dqa.db.model.BatchType;
import org.openimmunizationsoftware.dqa.db.model.IssueAction;
import org.openimmunizationsoftware.dqa.db.model.MessageBatch;
import org.openimmunizationsoftware.dqa.db.model.SubmitterProfile;
import org.openimmunizationsoftware.dqa.web.DqaBasePage;
import org.openimmunizationsoftware.dqa.web.DqaSession;
import org.openimmunizationsoftware.dqa.web.NavigationPanel;

public class DataReceivedPage extends DqaBasePage
{

  public DataReceivedPage(final PageParameters parameters) {
    super(parameters, NavigationPanel.BATCH);

    DqaSession webSession = (DqaSession) getSession();
    SubmitterProfile submitterProfile = webSession.getSubmitterProfile();

    Session dataSession = webSession.getDataSession();
    Query query = dataSession.createQuery("from MessageBatch where profile = ? and batchType = ?");
    query.setParameter(0, submitterProfile);
    query.setParameter(1, BatchType.SUBMISSION);
    List<MessageBatch> messageBatchList = query.list();
    for (MessageBatch messageBatch : messageBatchList)
    {
      query = dataSession.createQuery("from BatchReport where messageBatch = ?");
      query.setParameter(0, messageBatch);
      List<BatchReport> reportList = query.list();
      if (reportList.size() > 0)
      {
        messageBatch.setBatchReport(reportList.get(0));
      }

      query = dataSession.createQuery("from BatchActions where messageBatch = ?");
      query.setParameter(0, messageBatch);
      List<BatchActions> batchActionsList = query.list();
      for (BatchActions batchActions : batchActionsList)
      {
        messageBatch.getBatchActions(batchActions.getIssueAction()).setActionCount(batchActions.getActionCount());
      }

    }

    ListView<MessageBatch> messageBatchItems = new ListView<MessageBatch>("messageBatchItems", messageBatchList) {

      @Override
      protected void populateItem(ListItem<MessageBatch> item)
      {
        final MessageBatch messageBatch = item.getModelObject();

        int messageCount = messageBatch.getBatchReport().getMessageCount();
        BatchActions batchActionAccept = messageBatch.getBatchActions(IssueAction.ACCEPT);
        BatchActions batchActionError = messageBatch.getBatchActions(IssueAction.ERROR);
        BatchActions batchActionWarn = messageBatch.getBatchActions(IssueAction.WARN);
        BatchActions batchActionSkipped = messageBatch.getBatchActions(IssueAction.SKIP);
        SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy h:mm a z");
        int batchIdSelected = ((DqaSession) getSession()).getBatchId();
        boolean selected = batchIdSelected == messageBatch.getBatchId();
        item.add(makeSelected(new Label("date", sdf.format(messageBatch.getStartDate())), selected));
        item.add(makeSelected(new Label("score", "" + messageBatch.getBatchReport().getOverallScore()), selected));
        item.add(makeSelected(new Label("size", "" + messageCount), selected));
        item.add(makeSelected(new Label("acceptPer", makePercent(batchActionAccept.getActionCount(), messageCount)), selected));
        item.add(makeSelected(new Label("warnPer", makePercent(batchActionWarn.getActionCount(), messageCount)), selected));
        item.add(makeSelected(new Label("skippedPer", makePercent(batchActionSkipped.getActionCount(), messageCount)), selected));
        item.add(makeSelected(new Label("erroredPer", makePercent(batchActionError.getActionCount(), messageCount)), selected));

        Link link = new Link("dataReceivedSelectLink") {
          @Override
          public void onClick()
          {
            DqaSession webSession = (DqaSession) getSession();
            webSession.setBatchId(messageBatch.getBatchId());
            setResponsePage(new ErrorsWarningsPage(parameters));
          }
        };
        item.add(link);
      }

      private String makePercent(int num, int denom)
      {
        if (denom == 0 || num == 0)
        {
          return "";
        }
        return ((int) (100.0 * num / denom + 0.5)) + "%";

      }
    };
    add(messageBatchItems);

  }
}
