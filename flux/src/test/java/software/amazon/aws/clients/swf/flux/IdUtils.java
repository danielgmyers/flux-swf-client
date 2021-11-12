package software.amazon.aws.clients.swf.flux;

import java.util.Random;

public final class IdUtils {

    private static final Random rand = new Random();
    private static final String ID_CHARS = "0123456789abcdefghijklmnopqrstuvwxyz";

    public static String randomId(int idLength) {
        StringBuilder sb = new StringBuilder(idLength);
        for (int i = 0; i < idLength; i++) {
            sb.append(ID_CHARS.charAt(rand.nextInt(ID_CHARS.length())));
        }
        return sb.toString();
    }
}
