package com.johngodoi;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StringUtils {

    static List<String> splitLinesOnWords(String s) {
        Stream<String> listStream = Arrays.stream(replace(Arrays.asList("]","[","{","}","(",")","\""),s)
                .split("[ ,:/><]"));
        return listStream.collect(Collectors.toList());
    }

    static String replace(List<String> patterns, String in){
        if(patterns.isEmpty()) return in;
        return replace(patterns.subList(1, patterns.size()), in.replace(patterns.get(0), ""));
    }
}
