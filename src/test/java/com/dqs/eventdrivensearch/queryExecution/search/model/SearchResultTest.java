package com.dqs.eventdrivensearch.queryExecution.search.model;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SearchResultTest {

    @Test
    public void mergeSearchResultsWithNothing() {
        SearchResult searchResult = new SearchResult(new ArrayList<>() {{
            add("document1");
            add("document2");
        }}, 10, 3, 3);
        searchResult.mergeFrom(null);

        assertEquals(List.of("document1", "document2"), searchResult.documentIds());
        assertEquals(10, searchResult.total());
        assertEquals(3, searchResult.totalHits());
        assertEquals(3, searchResult.totalMatchedDocuments());
    }

    @Test
    public void mergeSearchResults() {
        SearchResult searchResult = new SearchResult(new ArrayList<>() {{
            add("document1");
            add("document2");
        }}, 10, 3, 3);
        searchResult.mergeFrom(new SearchResult(new ArrayList<>() {{
            add("document3");
            add("document4");
        }}, 8, 4, 4));

        assertEquals(List.of("document1", "document2", "document3", "document4"), searchResult.documentIds());
        assertEquals(18, searchResult.total());
        assertEquals(7, searchResult.totalHits());
        assertEquals(7, searchResult.totalMatchedDocuments());
    }
}