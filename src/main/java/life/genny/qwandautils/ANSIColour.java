/**
 * A Class for storing ANSI text colours 
 * used in logging.
 */

package life.genny.qwandautils;


import org.jboss.logging.Logger;

public class ANSIColour {

	private static final Logger log = Logger.getLogger(ANSIColour.class);

    public static final String RESET = "\033[0m";
    public static final String BLACK = "\033[0;30m";
    public static final String RED = "\033[0;31m";
    public static final String GREEN = "\033[0;32m";
    public static final String YELLOW = "\033[0;33m";
    public static final String BLUE = "\033[0;34m";
    public static final String PURPLE = "\033[0;35m";
    public static final String CYAN = "\033[0;36m";
    public static final String WHITE = "\033[0;37m";

	public static void logError(String msg) {
		log.error(RED+msg+RESET);
	}

	public static void logSuccess(String msg) {
		log.info(GREEN+msg+RESET);
	}

}
