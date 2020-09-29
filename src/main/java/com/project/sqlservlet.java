//TODO:
//1. Write logic for GroupBy in hadoop
//2. Write logic for SparkGroupBy 
//3. See parse function
package com.project;
import com.project.models.Input;
import com.project.models.Output;
import com.project.jobUtils.GroupBy;
import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.sql.SQLException;
import org.codehaus.jackson.map.ObjectMapper;

@WebServlet(
        name = "sql",
        urlPatterns = "/sql"
)
public class sqlservlet extends HttpServlet{
	@Override
	protected void doPost(HttpServletRequest req,HttpServletResponse res) throws ServletException,IOException{
		String select = req.getParameter("select");
		String from = req.getParameter("from");
		String where = req.getParameter("where");
		String groupBy = req.getParameter("groupBy");
		String having = req.getParameter("having");
		String result_json="";
		try{
			Input input = new Input(select,from,where,groupBy,having);
			input.parse();
			Output output = GroupBy.execute(input);
			// SparkGroupBy.execute(input,output); //figure it out
			//I have output POJO
			ObjectMapper obj = new ObjectMapper();
			result_json = obj.writeValueAsString(output);
		}catch(IOException e){
			e.printStackTrace();
		}catch(InterruptedException e){
			e.printStackTrace();
		}catch(ClassNotFoundException e){
			e.printStackTrace();
		}catch(SQLException e){
			e.printStackTrace();
		}
		req.setAttribute("results",result_json);
		RequestDispatcher view = req.getRequestDispatcher("result.jsp");
		view.forward(req,res);
	//doGet
	}
}	
