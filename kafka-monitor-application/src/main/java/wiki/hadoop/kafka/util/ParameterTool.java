package wiki.hadoop.kafka.util;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Jast
 * @description
 * @date 2023-04-10 17:02
 */
public class ParameterTool {
    private static Map<String, String> argsMap;

    private ParameterTool() {}

    public static void parse(String[] args) {
        argsMap = new HashMap<String, String>();
        for (int i = 0; i < args.length; i++) {
            if (args[i].startsWith("--")) {
                String key = args[i].substring(2);
                if (i + 1 < args.length && !args[i + 1].startsWith("--")) {
                    argsMap.put(key, args[i + 1]);
                    i++;
                } else {
                    argsMap.put(key, "");
                }
            }
        }
    }

    public static String get(String key) {
        return argsMap.get(key);
    }
}
