package com.dqs.eventdrivensearch.queryExecution.searchV2;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.Query;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_SINGLETON)
public class EmailIndex {

    private final String[] EMAIL_FIELDS = {"body", "subject", "date", "from", "to", "cc", "bcc"};

    private final StandardAnalyzer analyzer = new StandardAnalyzer();

    public Query getQuery(String queryString) throws ParseException {
        MultiFieldQueryParser parser = new MultiFieldQueryParser(EMAIL_FIELDS, analyzer);
        return parser.parse(queryString);
    }
}
