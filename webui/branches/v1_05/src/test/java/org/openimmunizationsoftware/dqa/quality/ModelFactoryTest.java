package org.openimmunizationsoftware.dqa.quality;

import junit.framework.TestCase;

import org.openimmunizationsoftware.dqa.quality.model.ModelFactory;
import org.openimmunizationsoftware.dqa.quality.model.ModelForm;

public class ModelFactoryTest extends TestCase
{
  public void testCreateModelSection() throws Exception
  {
    ModelForm modelForm = ModelFactory.getModelFormDefault();
    assertEquals(3, modelForm.getSections().size());
    assertEquals(3, modelForm.getSections().get(0).getSections().size());
  }
}
