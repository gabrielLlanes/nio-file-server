package server.status;

public enum RegistrationStatus {
    READ_ID,
    READ_FILE_SIZE,
    WRITE_POSITION,
    VERIFY_POSITION,
    FINALIZE_NEW,
    FINALIZE_RESTABLISH
}
