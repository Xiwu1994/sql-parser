package com.sql.parse.bean;

import java.util.ArrayList;
import java.util.List;
import lombok.Data;

@Data
public class ParseJoinOnRelation {
    List<ParseJoinOnSingleRelation> onColumnList = new ArrayList<>();

    public void setOnOneResult(ParseJoinOnSingleRelation parseJoinOnSingleRelation) {
        onColumnList.add(parseJoinOnSingleRelation);
    }
}
