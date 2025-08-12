package com.dqs.eventdrivensearch.queryExecution.searchV2;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.Query;


public class EmailIndex {

    public Query getQuery(String queryString) throws ParseException {
         final String[] EMAIL_FIELDS = {"body", "subject", "date", "from", "to", "cc", "bcc"};

         final StandardAnalyzer analyzer = new StandardAnalyzer();

        MultiFieldQueryParser parser = new MultiFieldQueryParser(EMAIL_FIELDS, analyzer);
        return parser.parse(queryString);
    }
}
