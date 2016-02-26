package org.openimmunizationsoftware.dqa.transport;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public abstract class CDCWSDLServer
{
  private static final String CDATA_END = "]]>";
  private static final String CDATA_START = "<![CDATA[";

  public void process(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException
  {
    String xmlMessage = getBody(req);
    resp.setContentType("application/soap+xml; charset=UTF-8; action=\"urn:cdc:iisb:2011:connectivityTest\"");
    PrintWriter out = new PrintWriter(resp.getOutputStream());
    try
    {
      if (isConnectivityTest(xmlMessage))
      {
        String echoBack = getEchoBack(xmlMessage);
        out.println("<?xml version='1.0' encoding='UTF-8'?>");
        out.println("<Envelope xmlns=\"http://www.w3.org/2003/05/soap-envelope\">");
        out.println("  <Body>");
        out.println("    <connectivityTestResponse xmlns=\"urn:cdc:iisb:2011\">");
        out.println("      <return>" + getEchoBackMessage(echoBack) + "</return>   ");
        out.println("    </connectivityTestResponse>");
        out.println("  </Body>");
        out.println("</Envelope>");
      } else if (isSubmitSingleMessage(xmlMessage))
      {
        SubmitSingleMessage ssm = getSubmitSingleMessage(xmlMessage);
        authorize(ssm);
        if (ssm.getAccessDenied() == null)
        {
          out.println("<?xml version='1.0' encoding='UTF-8'?>");
          out.println("<Envelope xmlns=\"http://www.w3.org/2003/05/soap-envelope\">");
          out.println("  <Header />");
          out.println("   <Body>");
          out.println("      <submitSingleMessageResponse xmlns=\"urn:cdc:iisb:2011\">");
          out.print("        <return><![CDATA[");
          process(ssm, out);
          out.println("]]></return>");
          out.println("      </submitSingleMessageResponse>");
          out.println("  </Body>");
          out.println("</Envelope>");
        } else
        {
          doAccessDenied(out, ssm);
        }
        if (ssm.isDebug())
        {
          // printDebugOutput(out, session, profile, qualityCollector);
        }
      } else
      {
        doNotSupported(out);
      }
    } catch (Exception e)
    {
      doFault(out, e);
    }
    finally {
      out.close();
    }
  }

  public abstract void authorize(SubmitSingleMessage ssm);

  public abstract void process(SubmitSingleMessage ssm, PrintWriter out) throws IOException;

  public abstract String getEchoBackMessage(String message);

  private static String getBody(HttpServletRequest req) throws IOException
  {

    String body = null;
    StringBuilder stringBuilder = new StringBuilder();
    BufferedReader bufferedReader = null;

    try
    {
      InputStream inputStream = req.getInputStream();
      if (inputStream != null)
      {
        bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        char[] charBuffer = new char[128];
        int bytesRead = -1;
        while ((bytesRead = bufferedReader.read(charBuffer)) > 0)
        {
          stringBuilder.append(charBuffer, 0, bytesRead);
        }
      } else
      {
        stringBuilder.append("");
      }
    } catch (IOException ex)
    {
      throw ex;
    } finally
    {
      if (bufferedReader != null)
      {
        try
        {
          bufferedReader.close();
        } catch (IOException ex)
        {
          throw ex;
        }
      }
    }

    body = stringBuilder.toString();
    return body;
  }

  private boolean isConnectivityTest(String xmlMessage)
  {
    if (xmlMessage.indexOf("<connectivityTest") > 0 || xmlMessage.indexOf(":connectivityTest") > 0)
    {
      return true;
    }
    return false;
  }

  private boolean isSubmitSingleMessage(String xmlMessage)
  {
    if (xmlMessage.indexOf("<submitSingleMessage") > 0 || xmlMessage.indexOf(":submitSingleMessage") > 0)
    {
      return true;
    }
    return false;
  }

  private String getEchoBack(String xmlMessage)
  {
    int startPos = xmlMessage.indexOf("<connectivityTest");
    if (startPos < 0)
    {
      startPos = xmlMessage.indexOf(":connectivityTest");
    }
    if (startPos > 0)
    {
      startPos = xmlMessage.indexOf("echoBack", startPos);
      if (startPos > 0)
      {
        startPos = xmlMessage.indexOf(">", startPos);
        if (startPos > 0)
        {
          startPos++;
          int endPos = xmlMessage.indexOf("</", startPos);
          if (endPos > 0)
          {
            return xmlMessage.substring(startPos, endPos);
          }
        }
      }
    }
    return "";
  }

  private SubmitSingleMessage getSubmitSingleMessage(String xmlMessage)
  {
    SubmitSingleMessage submitSingleMessage = new SubmitSingleMessage();
    int startPos = xmlMessage.indexOf("<connectivityTest");
    if (startPos < 0)
    {
      startPos = xmlMessage.indexOf(":submitSingleMessage");
    }
    if (startPos > 0)
    {
      submitSingleMessage.setUsername(readField(xmlMessage, startPos, "username"));
      submitSingleMessage.setPassword(readField(xmlMessage, startPos, "password"));
      submitSingleMessage.setFacilityID(readField(xmlMessage, startPos, "facilityID"));
      submitSingleMessage.setHl7Message(readField(xmlMessage, startPos, "hl7Message"));
    }
    return submitSingleMessage;
  }

  private String readField(String xmlMessage, int startPos, String field)
  {
    String value = "";
    int fieldStartPos = xmlMessage.indexOf(field, startPos);
    if (fieldStartPos > 0)
    {
      fieldStartPos = xmlMessage.indexOf(">", fieldStartPos);
      if (fieldStartPos > 0)
      {
        fieldStartPos++;
        int fieldEndPos = xmlMessage.indexOf("<", fieldStartPos);
        if (fieldEndPos > 0)
        {
          value = xmlMessage.substring(fieldStartPos, fieldEndPos).trim();
          if (value.startsWith(CDATA_START) && value.endsWith(CDATA_END))
          {
            value = value.substring(CDATA_START.length(), value.length() - CDATA_END.length());
          } else
          {
            value = value.replaceAll("\\Q&amp;\\E", "&").replaceAll("\\Q&#xd;\\E", "\r");
          }
        }
      }
    }
    return value;
  }

  private void doAccessDenied(PrintWriter out, SubmitSingleMessage ssm)
  {
    out.println("<?xml version='1.0' encoding='UTF-8'?>");
    out.println("<Envelope xmlns=\"http://www.w3.org/2003/05/soap-envelope\">");
    out.println("   <Body>");
    out.println("      <Fault xmlns:ns4=\"http://schemas.xmlsoap.org/soap/envelope/\">");
    out.println("         <Code>");
    out.println("            <Value>Sender</Value>");
    out.println("         </Code>");
    out.println("         <Reason>");
    out.println("            <Text xml:lang=\"en\">" + ssm.getAccessDenied() + "</Text>");
    out.println("         </Reason>");
    out.println("         <Detail>");
    out.println("            <SecurityFault xmlns=\"urn:cdc:iisb:2011\">");
    out.println("               <Code>10</Code>");
    out.println(
        "               <Reason xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"xs:string\">Security</Reason>");
    out.println("               <Detail>Invalid Username or Password</Detail>");
    out.println("            </SecurityFault>");
    out.println("         </Detail>");
    out.println("      </Fault>");
    out.println("   </Body>");
    out.println("</Envelope>");
  }

  private void doNotSupported(PrintWriter out)
  {
    out.println("<?xml version='1.0' encoding='UTF-8'?>");
    out.println("<Envelope xmlns=\"http://www.w3.org/2003/05/soap-envelope\">");
    out.println("   <Body>");
    out.println("      <Fault xmlns:ns4=\"http://schemas.xmlsoap.org/soap/envelope/\">");
    out.println("         <Code>");
    out.println("            <Value>Sender</Value>");
    out.println("         </Code>");
    out.println("         <Reason>");
    out.println("            <Text xml:lang=\"en\">Unrecognized request</Text>");
    out.println("         </Reason>");
    out.println("         <Detail>");
    out.println("            <UnsupportedOperationFault xmlns=\"urn:cdc:iisb:2011\">");
    out.println("               <Code>8675309</Code>");
    out.println(
        "               <Reason xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"xs:string\">UnsupportedOperation</Reason>");
    out.println("               <Detail>This Operation is not supported</Detail>");
    out.println("            </UnsupportedOperationFault>");
    out.println("         </Detail>");
    out.println("      </Fault>");
    out.println("   </Body>");
    out.println("</Envelope>");
  }

  private void doFault(PrintWriter out, Exception e)
  {
    out.println("<?xml version='1.0' encoding='UTF-8'?>");
    out.println("<Envelope xmlns=\"http://www.w3.org/2003/05/soap-envelope\">");
    out.println("   <Body>");
    out.println("      <Fault xmlns:ns4=\"http://schemas.xmlsoap.org/soap/envelope/\">");
    out.println("         <Code>");
    out.println("            <Value>Sender</Value>");
    out.println("         </Code>");
    out.println("         <Reason>");
    out.println("            <Text xml:lang=\"en\">" + e.getMessage() + "</Text>");
    out.println("         </Reason>");
    out.println("         <Detail>");
    out.println("            <fault xmlns=\"urn:cdc:iisb:2011\">");
    out.println("               <Code>54353</Code>");
    out.println(
        "               <Reason xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"xs:string\">My Reason</Reason>");
    out.println("               <Detail>An unexpected problem occured</Detail>");
    out.println("            </fault>");
    out.println("         </Detail>");
    out.println("      </Fault>");
    out.println("   </Body>");
    out.println("</Envelope>");
  }
}
