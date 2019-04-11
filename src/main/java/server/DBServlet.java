package server;

import com.google.gson.Gson;
import server.responses.ErrorResponse;
import server.responses.SuccessResponse;
import store.Store;
import server.responses.GetResponse;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.ServletException;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

public class DBServlet extends HttpServlet {

    private Store store;
    private Gson gson = new Gson();

    DBServlet(Store store) {
        this.store = store;
    }

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

        response.setContentType("application/json");

        String[] keys = request.getParameterMap().get("key");

        if (keys == null || keys.length != 1) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            response.getWriter().println(gson.toJson(
                    new ErrorResponse("exactly one key must be provided")));
            return;
        }
        String key = keys[0];
        try {
            String value = store.get(key);
            if (value == null) {
                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                response.getWriter().println(gson.toJson(
                        new ErrorResponse("key " + key + " not found")));
                return;
            }
            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().println(gson.toJson(new GetResponse(key, value)));
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }

    }

    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

        response.setContentType("application/json");

        Set<Map.Entry<String, String[]>> query = request.getParameterMap().entrySet();

        String key = null;
        String value = null;

        for (Map.Entry entry : request.getParameterMap().entrySet()) {
            key = (String) entry.getKey();
            for (String entryValue : (String[]) entry.getValue()) {
                value = entryValue;
            }
        }

        if (key == null || value == null) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            response.getWriter().println(gson.toJson(
                    new ErrorResponse("a key and value must be provided")));
            return;
        }

        try {
            store.put(key, value);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }

        response.setStatus(HttpServletResponse.SC_OK);
        response.getWriter().println(gson.toJson(new SuccessResponse()));
    }

    protected void doDelete(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

        response.setContentType("application/json");

        String[] keys = request.getParameterMap().get("key");

        if (keys == null || keys.length != 1) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            response.getWriter().println(gson.toJson(
                    new ErrorResponse("exactly one key must be provided")));
            return;
        }
        String key = keys[0];
        store.delete(key);
        response.setStatus(HttpServletResponse.SC_OK);
        response.getWriter().println(gson.toJson(new SuccessResponse()));
    }

}
