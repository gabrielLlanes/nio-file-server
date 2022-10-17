package server.openoptionchoices;

import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.Set;

public class OpenOptionChoices {

    /*
     * for clients who send a name of a file that does not already exist in the
     * filesystem, create a new file
     */
    public static final Set<OpenOption> OPEN_NEW;

    /*
     * for clients whose connection was closed in the middle of data transfer,
     * append the existing file upon connection reestablishment
     */
    public static final Set<OpenOption> APPEND_EXISTING;

    /*
     * for clients who send a name of a file that already exists in the filesystem,
     * truncate the existing file
     */
    public static final Set<OpenOption> TRUNCATE_EXISTING;

    static {
        OPEN_NEW = new HashSet<>();
        OPEN_NEW.add(StandardOpenOption.CREATE_NEW);
        OPEN_NEW.add(StandardOpenOption.WRITE);
        APPEND_EXISTING = new HashSet<>();
        APPEND_EXISTING.add(StandardOpenOption.WRITE);
        APPEND_EXISTING.add(StandardOpenOption.APPEND);
        TRUNCATE_EXISTING = new HashSet<>();
        TRUNCATE_EXISTING.add(StandardOpenOption.WRITE);
        TRUNCATE_EXISTING.add(StandardOpenOption.TRUNCATE_EXISTING);
    }
}
