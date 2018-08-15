package spark.example.spark.example;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.search.join.ToParentBlockJoinQuery;
import org.apache.lucene.store.FSDirectory;

public class SparkLucene {
	IndexReader reader;
	URI indexLocation;

	public SparkLucene() {
		try {
			indexLocation = new URI(
					"file:///Users/m029206/git/lexevs/lexevs-dao/resources/lbIndex/SNOMED%20Clinical%20Terms%20US%20Edition-2016_09_01");
			reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexLocation)));
		} catch (URISyntaxException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public String getResponse(String queryText) throws ParseException, IOException {

		final IndexSearcher searcher = new IndexSearcher(reader);
		Analyzer analyzer = new StandardAnalyzer();
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        Term baseQuery = new Term("propertyType", "presentation");
        Term preferred = new Term("isPreferred","T");
        builder.add(new TermQuery(baseQuery), Occur.MUST);
        builder.add(new TermQuery(preferred), Occur.MUST);
		QueryParser parser = new QueryParser("propertyValue", analyzer);
		builder.add(parser.parse(queryText), Occur.MUST);
		TermQuery termQuery = new TermQuery(new Term("isParentDoc", "true"));
		ToParentBlockJoinQuery query = new ToParentBlockJoinQuery(builder.build(),
				new QueryBitSetProducer(termQuery), ScoreMode.Total);

		TopDocs result = searcher.search(query, 10);
		
		return Arrays.asList(result.scoreDocs).stream().map(x -> {
			try {
				return searcher.doc(x.doc).get("entityDescription");
			} catch (IOException e) {
				e.printStackTrace();
			}
			return null;
		}).collect(Collectors.joining("<br/>"));		
	}

	public IndexReader getReader() {
		return reader;
	}

	public void setReader(IndexReader reader) {
		this.reader = reader;
	}

}
