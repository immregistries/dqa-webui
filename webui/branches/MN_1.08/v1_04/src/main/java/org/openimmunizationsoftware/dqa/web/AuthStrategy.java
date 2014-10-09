package org.openimmunizationsoftware.dqa.web;

import org.apache.wicket.Component;
import org.apache.wicket.RestartResponseAtInterceptPageException;
import org.apache.wicket.Session;
import org.apache.wicket.authorization.Action;
import org.apache.wicket.authorization.IAuthorizationStrategy;
import org.apache.wicket.request.component.IRequestableComponent;

public class AuthStrategy implements IAuthorizationStrategy
{

  public <T extends IRequestableComponent> boolean isInstantiationAuthorized(Class<T> componentClass)
  {
    if (SecurePage.class.isAssignableFrom(componentClass))
    {
      DqaSession webSession = (DqaSession) Session.get();
      if (webSession.getUsername() == null)
      {
        throw new RestartResponseAtInterceptPageException(LoginPage.class);
      }
    }
    return true;
  }

  public boolean isActionAuthorized(Component component, Action action)
  {
    return true;
  }

}
