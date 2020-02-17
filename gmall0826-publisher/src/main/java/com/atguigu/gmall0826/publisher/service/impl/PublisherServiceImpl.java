package com.atguigu.gmall0826.publisher.service.impl;

import com.atguigu.gmall0826.common.constant.GmallConstant;
import com.atguigu.gmall0826.publisher.mapper.DauMapper;
import com.atguigu.gmall0826.publisher.mapper.OrderMapper;
import com.atguigu.gmall0826.publisher.service.PublisherService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import org.apache.lucene.queryparser.xml.builders.BooleanQueryBuilder;
import org.apache.lucene.search.BooleanQuery;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
   DauMapper dauMapper;

    @Autowired
    OrderMapper orderMapper;

    @Autowired
    JestClient jestClient;



    @Override
    public Long getDauTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }

    @Override
    public Map getDauHourCount(String date) {  //调整变换结构
        //   maplist ===> [{"loghour":"12","ct",500},{"loghour":"11","ct":400}......]
        List<Map> mapList = dauMapper.selectDauHourCount(date);
        // hourCountMap ==> {"12":500,"11":400,......}
        Map hourCountMap=new HashMap();
        for (Map map : mapList) {
            hourCountMap.put ( map.get("LOGHOUR"),map.get("CT"));
        }
        return hourCountMap;
    }

    @Override
    public Double getOrderAmount(String date) {
        return orderMapper.selectOrderAmount(date);
    }

    @Override
    public Map getOrderAmountHour(String date) {
        List<Map> mapList = orderMapper.selectOrderAmountHour(date);
        Map hourAmountMap=new HashMap();
        for (Map map : mapList) {
            hourAmountMap.put ( map.get("CREATE_HOUR"),map.get("ORDER_AMOUNT"));
        }
        return hourAmountMap;

    }

    @Override
    public Map getSaleDetail(String date, String keyword, int pageNo, int pagesize) {
       String query="{\n" +
               "  \"query\": {\n" +
               "    \"bool\": {\n" +
               "      \"filter\": {\n" +
               "        \"term\": {\n" +
               "          \"dt\": \"2020-02-17\"\n" +
               "        }\n" +
               "      },\n" +
               "      \"must\": [\n" +
               "        {\"match\": {\n" +
               "          \"sku_name\":{\n" +
               "            \"query\": \"小米手机\",\n" +
               "            \"operator\": \"and\"\n" +
               "          }\n" +
               "        }}\n" +
               "      ]\n" +
               "    }\n" +
               "  }\n" +
               "  , \n" +
               "  \"aggs\": {\n" +
               "    \"groupby_age\": {\n" +
               "      \"terms\": {\n" +
               "        \"field\": \"user_age\",\n" +
               "        \"size\": 120\n" +
               "      }\n" +
               "    },\n" +
               "    \"groupby_gender\":{\n" +
               "      \"terms\": {\n" +
               "        \"field\": \"user_gender\",\n" +
               "        \"size\": 2\n" +
               "      }\n" +
               "    }\n" +
               "  }\n" +
               "  ,\n" +
               "  \"from\": 0\n" +
               "  , \"size\": 10\n" +
               "}";

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //查询条件
        BoolQueryBuilder  boolQueryBuilder= new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("dt",date));
        boolQueryBuilder.must(new MatchQueryBuilder("sku_name",keyword).operator(MatchQueryBuilder.Operator.AND));
        searchSourceBuilder.query(boolQueryBuilder);

        //聚合
        //年龄
        TermsBuilder ageBuilder = AggregationBuilders.terms("groupby_age").field("user_age").size(120);
        searchSourceBuilder.aggregation(ageBuilder);
        //性别
        TermsBuilder genderBuilder = AggregationBuilders.terms("groupby_gender").field("user_gender").size(2);
        searchSourceBuilder.aggregation(genderBuilder);

        //分页
        int rowNo=(pageNo-1)*pagesize;
        searchSourceBuilder.from(rowNo);
        searchSourceBuilder.size(pagesize);

        System.out.println(searchSourceBuilder.toString());

        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstant.ES_INDEX_SALE).addType("_doc").build();
        try {
            SearchResult searchResult = jestClient.execute(search);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }


}
