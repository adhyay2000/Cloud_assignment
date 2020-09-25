package com.sample;
import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

@WebServelt{
	name="sql";
	urlPatterns = "/sql";
}
public class sqlservlet extends HttpServlet{
	@Override
	protected void doPost(HttpServletRequest req,HttpServletResponse res) throws ServletException,IOException{
		String SQL = req.getParameter("sql");
		// PARSE SQL AND CONVERT TO JSON
		// GET THE ANSWER IN result_json
		req.setAttribute("results",result_json);
		RequestDispatcher view = req.getRequestDispatcher("result.jsp");
		view.forward(req,resp);
	//doGet
}	
