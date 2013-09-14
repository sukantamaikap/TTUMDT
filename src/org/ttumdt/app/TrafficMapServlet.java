package org.ttumtd.app;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Servlet implementation class TrafficMapServlet
 */
public class TrafficMapServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public TrafficMapServlet() {
        super();
        // TODO Auto-generated constructor stub
    }

	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		doPost(request,response);
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

		try {
		    // Set the attribute and Forward to hello.jsp
		    //getServletConfig().getServletContext().getRequestDispatcher("/jsp/map_drawwaypt2.html").forward(request, response);
		    
		    StringBuilder sbr = new StringBuilder();
		    /*sbr.append("From Lat Lng:").append(request.getParameter("fromLatLng")).append("\n")
		    .append("To Lat Lng:").append(request.getParameter("toLatLng")).append("\n")
		    .append("Start time:").append(getDate(request,"from")).append("\n")
		    .append("End Time:").append(getDate(request,"to")).append("\n");   */
	        Map paramMap = request.getParameterMap();
	        System.out.println(paramMap);
	        for (Object param: paramMap.keySet()) {
	            sbr.append(param).append("-->").append(request.getParameter((String)param)).append("\n");
	        }

	        PrintWriter out = response.getWriter();
	        out.println(sbr.toString());
		   } catch (Exception ex) {
		       ex.printStackTrace ();
		   }

	}
	
    private String getDate (HttpServletRequest request, String datePrefix) {
        return request.getParameter(datePrefix + "Month") + "-" +
               request.getParameter(datePrefix + "Day") + "-" +
               request.getParameter(datePrefix + "Year") + "--" +
               request.getParameter(datePrefix + "Hr") + ":" +
               request.getParameter(datePrefix + "Min");
    }	

}
