package com.sql.parse.bean;

import java.util.Set;
import lombok.Data;

@Data
public class ParseJoinOnSingleRelation {
    Set<String> leftColumnSet;
    Set<String> rightColumnSet;
}
