package org.pentaho.di.trans.kafka.producer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;
import java.util.Map;

/**
 * Created by jfd on 7/3/17.
 */
public class DataCheck {

    @Autowired
    JdbcTemplate jdbcTemplate;

    public void topicValidate(){
        String sql = "";

        List<Map<String, Object>> topicList = jdbcTemplate.queryForList(sql);
    }
}
