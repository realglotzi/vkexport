package vkexport;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.Charset;
import java.util.Arrays;

public class MemoFieldHelper {

    // Memo pointer logic
    public static String readMemoViaPointer(final File dbfFile, final String dbtPath, final VkExportFull.DbfLayout layout, final int recno, final String fieldNameUpper, final Charset cs) throws IOException {
        if (layout == null || dbtPath == null) return null;
        final Integer off = layout.fieldOffset.get(fieldNameUpper);
        final Integer len = layout.fieldLength.get(fieldNameUpper);
        if (off == null || len == null) return null;
        final long pos = layout.headerLen + (long)(recno-1)*layout.recordLen + 1 + off; // skip delete flag
        try (final RandomAccessFile raf = new RandomAccessFile(dbfFile, "r")) {
            final byte[] ptrBytes = new byte[len];
            raf.seek(pos);
            raf.readFully(ptrBytes);
            final String ptrStr = new String(ptrBytes, java.nio.charset.StandardCharsets.US_ASCII).trim();
            int blockNo = 0;
            if (!ptrStr.isEmpty() && ptrStr.chars().allMatch(Character::isDigit)) {
                blockNo = Integer.parseInt(ptrStr);
            } else if (len >= 4) {
                blockNo = (ptrBytes[0]&0xFF) | ((ptrBytes[1]&0xFF)<<8) | ((ptrBytes[2]&0xFF)<<16) | ((ptrBytes[3]&0xFF)<<24);
            }
            if (blockNo <= 0) return null;
            final int blockSize = 512;
            final byte[] memo = readDbtBlocks(new File(dbtPath), blockNo, blockSize, 16);
            if (memo == null) return null;
            int end = memo.length;
            for (int i=0;i<memo.length;i++) { if (memo[i]==0x1A) { end=i; break; } }
            while (end>0 && (memo[end-1]==0 || memo[end-1]==0x1A)) end--;
            final String s = new String(memo, 0, end, cs);
            return s.replace("\r\n","\n");
        }
    }

    public static File resolveMemoFile(final File dbfFile) {
        final String name = dbfFile.getName();
        final int dot = name.lastIndexOf('.');
        final String base = (dot > 0 ? name.substring(0, dot) : name);
        final File dir = dbfFile.getParentFile() != null ? dbfFile.getParentFile() : new File(".");
        File dbt = new File(dir, base + ".DBT");
        if (!dbt.exists()) dbt = new File(dir, base + ".dbt");
        if (dbt.exists()) return dbt;
        return null;
    }

    private static byte[] readDbtBlocks(final File dbtFile, final int blockNo, final int blockSize, final int maxBlocks) throws IOException {
        if (!dbtFile.exists()) return null;
        try (final RandomAccessFile raf = new RandomAccessFile(dbtFile, "r")) {
            final long offset = (long)blockNo * blockSize;
            if (offset >= raf.length()) return null;
            final int toRead = (int)Math.min(raf.length()-offset, (long)blockSize*maxBlocks);
            final byte[] buf = new byte[toRead];
            raf.seek(offset);
            final int n = raf.read(buf);
            if (n < toRead) return Arrays.copyOf(buf, n);
            return buf;
        }
    }


}
