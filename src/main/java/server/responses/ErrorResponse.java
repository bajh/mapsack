package server.responses;

public class ErrorResponse extends StatusResponse {
    private String message;

    public ErrorResponse(String message) {
        super.status = "error";
        this.message = message;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getStatus() {
        return status;
    }
}
