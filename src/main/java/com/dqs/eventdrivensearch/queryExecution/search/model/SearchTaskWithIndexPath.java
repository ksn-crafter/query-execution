package com.dqs.eventdrivensearch.queryExecution.search.model;

import java.nio.file.Path;

public record SearchTaskWithIndexPath(Path indexPath, SearchTask task) {
}
