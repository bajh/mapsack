package server.responses;

public class SuccessResponse extends StatusResponse {
    public SuccessResponse() {
        super.status = "ok";
    }
}
