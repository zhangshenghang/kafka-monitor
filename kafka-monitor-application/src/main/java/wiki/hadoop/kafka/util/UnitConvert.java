package wiki.hadoop.kafka.util;

import java.text.DecimalFormat;

/**
 * @author Jast
 * @description
 * @date 2023-04-10 14:11
 */
public class UnitConvert {
    /**
     * @date 2023/4/7 下午3:25
     * @description 将字节数转换为易于阅读的字符串格式
     * @param bytes 字节数
     * @return 字符串格式的文件大小
     * @author Jast
     */
    public static String bytesToSize(long bytes) {
        if (bytes <= 0) {
            return "0B";
        }
        final String[] units = {"B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"};
        int digitGroups = (int) (Math.log10(bytes) / Math.log10(1024));
        return new DecimalFormat("#,##0.#").format(bytes / Math.pow(1024, digitGroups))
                + " "
                + units[digitGroups];
    }
}
