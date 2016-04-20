package cz.cvut.bigdata.wordcount;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;

import static cz.cvut.bigdata.wordcount.WordCount.WordCountMapper.DOCUMENT_COUNT_HELPER;

/**
 * Created by sange on 20/04/16.
 */
public class Util {

    public static ArrayList<String> readLines (URI uri) {
        ArrayList<String> list = new ArrayList<>();

        BufferedReader bfr = null;
        try {
            bfr = new BufferedReader(new FileReader(new File(uri.getPath()).getName()));

            // read vocabulary & store: word-wordId
            String line;
            while ((line = bfr.readLine()) != null) {
                list.add(line);
            }

            bfr.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

        return list;
    }
}
